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

#ifndef DAEMON_SERVER_H_
#define DAEMON_SERVER_H_

#include "PyModules.h"

#include <set>
#include <string>

#include "common/Mutex.h"
#include "common/LogClient.h"

#include <msg/Messenger.h>
#include <mon/MonClient.h>

#include "auth/AuthAuthorizeHandler.h"

#include "MgrSession.h"
#include "DaemonState.h"

class MMgrReport;
class MMgrOpen;
class MCommand;
struct MgrCommand;


/**
 * Server used in ceph-mgr to communicate with Ceph daemons like
 * MDSs and OSDs.
 */
class DaemonServer : public Dispatcher
{
protected:
  boost::scoped_ptr<Throttle> client_byte_throttler;
  boost::scoped_ptr<Throttle> client_msg_throttler;
  boost::scoped_ptr<Throttle> osd_byte_throttler;
  boost::scoped_ptr<Throttle> osd_msg_throttler;
  boost::scoped_ptr<Throttle> mds_byte_throttler;
  boost::scoped_ptr<Throttle> mds_msg_throttler;
  boost::scoped_ptr<Throttle> mon_byte_throttler;
  boost::scoped_ptr<Throttle> mon_msg_throttler;

  Messenger *msgr;
  MonClient *monc;
  DaemonStateIndex &daemon_state;
  ClusterState &cluster_state;
  PyModules &py_modules;
  LogChannelRef clog, audit_clog;

  AuthAuthorizeHandlerRegistry auth_registry;

  Mutex lock;

  static void _generate_command_map(map<string,cmd_vartype>& cmdmap,
                                    map<string,string> &param_str_map);
  static const MgrCommand *_get_mgrcommand(const string &cmd_prefix,
                                           MgrCommand *cmds, int cmds_size);
  bool _allowed_command(
    MgrSession *s, const string &module, const string &prefix,
    const map<string,cmd_vartype>& cmdmap,
    const map<string,string>& param_str_map,
    const MgrCommand *this_cmd);

private:
  friend class ReplyOnFinish;
  bool _reply(MCommand* m,
	      int ret, const std::string& s, const bufferlist& payload);

public:
  int init(uint64_t gid, entity_addr_t client_addr);
  void shutdown();

  entity_addr_t get_myaddr() const;

  DaemonServer(MonClient *monc_,
	       DaemonStateIndex &daemon_state_,
	       ClusterState &cluster_state_,
	       PyModules &py_modules_,
	       LogChannelRef cl,
	       LogChannelRef auditcl);
  ~DaemonServer() override;

  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new) override;
  bool ms_verify_authorizer(Connection *con,
      int peer_type,
      int protocol,
      ceph::bufferlist& authorizer,
      ceph::bufferlist& authorizer_reply,
      bool& isvalid,
      CryptoKey& session_key) override;

  bool handle_open(MMgrOpen *m);
  bool handle_report(MMgrReport *m);
  bool handle_command(MCommand *m);
};

#endif

