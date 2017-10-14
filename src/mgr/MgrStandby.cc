// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <Python.h>

#include "common/errno.h"
#include "mon/MonClient.h"
#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"

#include "mgr/MgrContext.h"

#include "messages/MMgrBeacon.h"
#include "messages/MMgrMap.h"
#include "Mgr.h"

#include "MgrStandby.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "


MgrStandby::MgrStandby() :
  Dispatcher(g_ceph_context),
  monc(new MonClient(g_ceph_context)),
  client_messenger(Messenger::create_client_messenger(g_ceph_context, "mgr")),
  objecter(new Objecter(g_ceph_context, client_messenger, monc, NULL, 0, 0)),
  log_client(g_ceph_context, client_messenger, &monc->monmap, LogClient::NO_FLAGS),
  clog(log_client.create_channel(CLOG_CHANNEL_CLUSTER)),
  audit_clog(log_client.create_channel(CLOG_CHANNEL_AUDIT)),
  lock("MgrStandby::lock"),
  timer(g_ceph_context, lock),
  active_mgr(nullptr)
{
}


MgrStandby::~MgrStandby()
{
  delete objecter;
  delete monc;
  delete client_messenger;
}

const char** MgrStandby::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    // clog & admin clog
    "clog_to_monitors",
    "clog_to_syslog",
    "clog_to_syslog_facility",
    "clog_to_syslog_level",
    "osd_objectstore_fuse",
    "clog_to_graylog",
    "clog_to_graylog_host",
    "clog_to_graylog_port",
    "host",
    "fsid",
    NULL
  };
  return KEYS;
}

void MgrStandby::handle_conf_change(
  const struct md_config_t *conf,
  const std::set <std::string> &changed)
{
  if (changed.count("clog_to_monitors") ||
      changed.count("clog_to_syslog") ||
      changed.count("clog_to_syslog_level") ||
      changed.count("clog_to_syslog_facility") ||
      changed.count("clog_to_graylog") ||
      changed.count("clog_to_graylog_host") ||
      changed.count("clog_to_graylog_port") ||
      changed.count("host") ||
      changed.count("fsid")) {
    _update_log_config();
  }
}

int MgrStandby::init()
{
  Mutex::Locker l(lock);

  // Initialize Messenger
  client_messenger->add_dispatcher_tail(this);
  client_messenger->start();

  // Initialize MonClient
  if (monc->build_initial_monmap() < 0) {
    client_messenger->shutdown();
    client_messenger->wait();
    return -1;
  }

  monc->sub_want("mgrmap", 0, 0);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD
      |CEPH_ENTITY_TYPE_MDS|CEPH_ENTITY_TYPE_MGR);
  monc->set_messenger(client_messenger);
  int r = monc->init();
  if (r < 0) {
    monc->shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  r = monc->authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify a mgr ID with a valid keyring?" << dendl;
    monc->shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }

  client_t whoami = monc->get_global_id();
  client_messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  monc->set_log_client(&log_client);
  _update_log_config();

  objecter->set_client_incarnation(0);
  objecter->init();
  client_messenger->add_dispatcher_head(objecter);
  objecter->start();

  timer.init();
  send_beacon();

  dout(4) << "Complete." << dendl;
  return 0;
}

void MgrStandby::send_beacon()
{
  assert(lock.is_locked_by_me());
  dout(1) << state_str() << dendl;
  dout(10) << "sending beacon as gid " << monc->get_global_id() << dendl;

  bool available = active_mgr != nullptr && active_mgr->is_initialized();
  auto addr = available ? active_mgr->get_server_addr() : entity_addr_t();
  MMgrBeacon *m = new MMgrBeacon(monc->get_fsid(),
				 monc->get_global_id(),
                                 g_conf->name.get_id(),
                                 addr,
                                 available);
                                 
  monc->send_mon_message(m);
  timer.add_event_after(g_conf->mgr_beacon_period, new FunctionContext(
        [this](int r){
          send_beacon();
        }
  )); 
}

void MgrStandby::handle_signal(int signum)
{
  Mutex::Locker l(lock);
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got signal " << sig_str(signum) << " ***" << dendl;
  shutdown();
}

void MgrStandby::shutdown()
{
  // Expect already to be locked as we're called from signal handler
  assert(lock.is_locked_by_me());

  if (active_mgr) {
    active_mgr->shutdown();
  }

  objecter->shutdown();

  timer.shutdown();

  monc->shutdown();
  client_messenger->shutdown();
}

void MgrStandby::_update_log_config()
{
  map<string,string> log_to_monitors;
  map<string,string> log_to_syslog;
  map<string,string> log_channel;
  map<string,string> log_prio;
  map<string,string> log_to_graylog;
  map<string,string> log_to_graylog_host;
  map<string,string> log_to_graylog_port;
  uuid_d fsid;
  string host;

  if (parse_log_client_options(cct, log_to_monitors, log_to_syslog,
			       log_channel, log_prio, log_to_graylog,
			       log_to_graylog_host, log_to_graylog_port,
			       fsid, host) == 0) {
    clog->update_config(log_to_monitors, log_to_syslog,
			log_channel, log_prio, log_to_graylog,
			log_to_graylog_host, log_to_graylog_port,
			fsid, host);
    audit_clog->update_config(log_to_monitors, log_to_syslog,
			      log_channel, log_prio, log_to_graylog,
			      log_to_graylog_host, log_to_graylog_port,
			      fsid, host);
  }
}

void MgrStandby::handle_mgr_map(MMgrMap* mmap)
{
  auto map = mmap->get_map();
  dout(4) << "received map epoch " << map.get_epoch() << dendl;
  const bool active_in_map = map.active_gid == monc->get_global_id();
  dout(4) << "active in map: " << active_in_map
          << " active is " << map.active_gid << dendl;
  if (active_in_map) {
    if (!active_mgr) {
      dout(1) << "Activating!" << dendl;
      active_mgr.reset(new Mgr(monc, client_messenger, objecter, clog, audit_clog));
      active_mgr->background_init();
      dout(1) << "I am now active" << dendl;
    } else {
      dout(10) << "I was already active" << dendl;
    }
  } else {
    if (active_mgr != nullptr) {
      derr << "I was active but no longer am" << dendl;
      active_mgr->shutdown();
      active_mgr.reset();
    }
  }
}

bool MgrStandby::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);
  dout(4) << state_str() << " " << *m << dendl;

  switch (m->get_type()) {
    case MSG_MGR_MAP:
      handle_mgr_map(static_cast<MMgrMap*>(m));
      break;

    default:
      if (active_mgr) {
        return active_mgr->ms_dispatch(m);
      } else {
        return false;
      }
  }

  m->put();
  return true;
}


bool MgrStandby::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->build_authorizer(dest_type);
  return *authorizer != NULL;
}

bool MgrStandby::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

// A reference for use by the signal handler
MgrStandby *signal_mgr = nullptr;

static void handle_mgr_signal(int signum)
{
  if (signal_mgr) {
    signal_mgr->handle_signal(signum);
  }
}

int MgrStandby::main(vector<const char *> args)
{
  // Enable signal handlers
  signal_mgr = this;
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_mgr_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mgr_signal);

  client_messenger->wait();

  // Disable signal handlers
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_mgr_signal);
  unregister_async_signal_handler(SIGTERM, handle_mgr_signal);
  shutdown_async_signal_handler();
  signal_mgr = nullptr;

  return 0;
}


std::string MgrStandby::state_str()
{
  return active_mgr == nullptr ? "standby" : "active";
}

