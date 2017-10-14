// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/bind.hpp>

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/ceph_argparse.h"
#include "common/code_environment.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "global/global_context.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/Watcher.h"
#include "librbd/api/Mirror.h"
#include "InstanceReplayer.h"
#include "InstanceWatcher.h"
#include "LeaderWatcher.h"
#include "Replayer.h"
#include "Threads.h"
#include "pool_watcher/RefreshImagesRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::Replayer: " \
                           << this << " " << __func__ << ": "

using std::chrono::seconds;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

using librbd::cls_client::dir_get_name;
using librbd::util::create_async_context_callback;

namespace rbd {
namespace mirror {

namespace {

class ReplayerAdminSocketCommand {
public:
  virtual ~ReplayerAdminSocketCommand() {}
  virtual bool call(Formatter *f, stringstream *ss) = 0;
};

class StatusCommand : public ReplayerAdminSocketCommand {
public:
  explicit StatusCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->print_status(f, ss);
    return true;
  }

private:
  Replayer *replayer;
};

class StartCommand : public ReplayerAdminSocketCommand {
public:
  explicit StartCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->start();
    return true;
  }

private:
  Replayer *replayer;
};

class StopCommand : public ReplayerAdminSocketCommand {
public:
  explicit StopCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->stop(true);
    return true;
  }

private:
  Replayer *replayer;
};

class RestartCommand : public ReplayerAdminSocketCommand {
public:
  explicit RestartCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->restart();
    return true;
  }

private:
  Replayer *replayer;
};

class FlushCommand : public ReplayerAdminSocketCommand {
public:
  explicit FlushCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->flush();
    return true;
  }

private:
  Replayer *replayer;
};

class LeaderReleaseCommand : public ReplayerAdminSocketCommand {
public:
  explicit LeaderReleaseCommand(Replayer *replayer) : replayer(replayer) {}

  bool call(Formatter *f, stringstream *ss) override {
    replayer->release_leader();
    return true;
  }

private:
  Replayer *replayer;
};

class ReplayerAdminSocketHook : public AdminSocketHook {
public:
  ReplayerAdminSocketHook(CephContext *cct, const std::string &name,
			  Replayer *replayer) :
    admin_socket(cct->get_admin_socket()) {
    std::string command;
    int r;

    command = "rbd mirror status " + name;
    r = admin_socket->register_command(command, command, this,
				       "get status for rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StatusCommand(replayer);
    }

    command = "rbd mirror start " + name;
    r = admin_socket->register_command(command, command, this,
				       "start rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StartCommand(replayer);
    }

    command = "rbd mirror stop " + name;
    r = admin_socket->register_command(command, command, this,
				       "stop rbd mirror " + name);
    if (r == 0) {
      commands[command] = new StopCommand(replayer);
    }

    command = "rbd mirror restart " + name;
    r = admin_socket->register_command(command, command, this,
				       "restart rbd mirror " + name);
    if (r == 0) {
      commands[command] = new RestartCommand(replayer);
    }

    command = "rbd mirror flush " + name;
    r = admin_socket->register_command(command, command, this,
				       "flush rbd mirror " + name);
    if (r == 0) {
      commands[command] = new FlushCommand(replayer);
    }

    command = "rbd mirror leader release " + name;
    r = admin_socket->register_command(command, command, this,
                                       "release rbd mirror leader " + name);
    if (r == 0) {
      commands[command] = new LeaderReleaseCommand(replayer);
    }
  }

  ~ReplayerAdminSocketHook() override {
    for (Commands::const_iterator i = commands.begin(); i != commands.end();
	 ++i) {
      (void)admin_socket->unregister_command(i->first);
      delete i->second;
    }
  }

  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) override {
    Commands::const_iterator i = commands.find(command);
    assert(i != commands.end());
    Formatter *f = Formatter::create(format);
    stringstream ss;
    bool r = i->second->call(f, &ss);
    delete f;
    out.append(ss);
    return r;
  }

private:
  typedef std::map<std::string, ReplayerAdminSocketCommand*> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

} // anonymous namespace

struct Replayer::C_RefreshLocalImages : public Context {
  Replayer *replayer;
  Context *on_finish;
  ImageIds image_ids;

  C_RefreshLocalImages(Replayer *replayer, Context *on_finish)
    : replayer(replayer), on_finish(on_finish) {
  }

  void finish(int r) override {
    replayer->handle_refresh_local_images(r, std::move(image_ids), on_finish);
  }
};

Replayer::Replayer(Threads<librbd::ImageCtx> *threads,
                   std::shared_ptr<ImageDeleter> image_deleter,
                   ImageSyncThrottlerRef<> image_sync_throttler,
                   int64_t local_pool_id, const peer_t &peer,
                   const std::vector<const char*> &args) :
  m_threads(threads),
  m_image_deleter(image_deleter),
  m_image_sync_throttler(image_sync_throttler),
  m_lock(stringify("rbd::mirror::Replayer ") + stringify(peer)),
  m_peer(peer),
  m_args(args),
  m_local_pool_id(local_pool_id),
  m_pool_watcher_listener(this),
  m_asok_hook(nullptr),
  m_replayer_thread(this),
  m_leader_listener(this)
{
}

Replayer::~Replayer()
{
  delete m_asok_hook;

  m_stopping.set(1);
  {
    Mutex::Locker l(m_lock);
    m_cond.Signal();
  }
  if (m_replayer_thread.is_started()) {
    m_replayer_thread.join();
  }
  if (m_leader_watcher) {
    m_leader_watcher->shut_down();
  }
  if (m_instance_watcher) {
    m_instance_watcher->shut_down();
  }
  if (m_instance_replayer) {
    m_instance_replayer->shut_down();
  }

  assert(!m_pool_watcher);
}

bool Replayer::is_blacklisted() const {
  Mutex::Locker locker(m_lock);
  return m_blacklisted;
}

bool Replayer::is_leader() const {
  Mutex::Locker locker(m_lock);
  return m_leader_watcher && m_leader_watcher->is_leader();
}

int Replayer::init()
{
  dout(20) << "replaying for " << m_peer << dendl;

  int r = init_rados(g_ceph_context->_conf->cluster,
                     g_ceph_context->_conf->name.to_str(),
                     "local cluster", &m_local_rados);
  if (r < 0) {
    return r;
  }

  r = init_rados(m_peer.cluster_name, m_peer.client_name,
                 std::string("remote peer ") + stringify(m_peer),
                 &m_remote_rados);
  if (r < 0) {
    return r;
  }

  r = m_local_rados->ioctx_create2(m_local_pool_id, m_local_io_ctx);
  if (r < 0) {
    derr << "error accessing local pool " << m_local_pool_id << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  std::string local_mirror_uuid;
  r = librbd::cls_client::mirror_uuid_get(&m_local_io_ctx,
                                          &local_mirror_uuid);
  if (r < 0) {
    derr << "failed to retrieve local mirror uuid from pool "
         << m_local_io_ctx.get_pool_name() << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  r = m_remote_rados->ioctx_create(m_local_io_ctx.get_pool_name().c_str(),
                                   m_remote_io_ctx);
  if (r < 0) {
    derr << "error accessing remote pool " << m_local_io_ctx.get_pool_name()
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  dout(20) << "connected to " << m_peer << dendl;

  m_instance_replayer.reset(
    InstanceReplayer<>::create(m_threads, m_image_deleter,
                               m_image_sync_throttler, m_local_rados,
                               local_mirror_uuid, m_local_pool_id));
  m_instance_replayer->init();
  m_instance_replayer->add_peer(m_peer.uuid, m_remote_io_ctx);

  m_instance_watcher.reset(InstanceWatcher<>::create(m_local_io_ctx,
                                                     m_threads->work_queue,
                                                     m_instance_replayer.get()));
  r = m_instance_watcher->init();
  if (r < 0) {
    derr << "error initializing instance watcher: " << cpp_strerror(r) << dendl;
    return r;
  }

  m_leader_watcher.reset(new LeaderWatcher<>(m_threads, m_local_io_ctx,
                                             &m_leader_listener));
  r = m_leader_watcher->init();
  if (r < 0) {
    derr << "error initializing leader watcher: " << cpp_strerror(r) << dendl;
    return r;
  }

  m_replayer_thread.create("replayer");

  return 0;
}

int Replayer::init_rados(const std::string &cluster_name,
                         const std::string &client_name,
                         const std::string &description, RadosRef *rados_ref) {
  rados_ref->reset(new librados::Rados());

  // NOTE: manually bootstrap a CephContext here instead of via
  // the librados API to avoid mixing global singletons between
  // the librados shared library and the daemon
  // TODO: eliminate intermingling of global singletons within Ceph APIs
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (client_name.empty() || !iparams.name.from_str(client_name)) {
    derr << "error initializing cluster handle for " << description << dendl;
    return -EINVAL;
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY,
                                    CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  cct->_conf->cluster = cluster_name;

  // librados::Rados::conf_read_file
  int r = cct->_conf->parse_config_files(nullptr, nullptr, 0);
  if (r < 0) {
    derr << "could not read ceph conf for " << description << ": "
	 << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }
  cct->_conf->parse_env();

  // librados::Rados::conf_parse_env
  std::vector<const char*> args;
  env_to_vec(args, nullptr);
  r = cct->_conf->parse_argv(args);
  if (r < 0) {
    derr << "could not parse environment for " << description << ":"
         << cpp_strerror(r) << dendl;
    cct->put();
    return r;
  }

  if (!m_args.empty()) {
    // librados::Rados::conf_parse_argv
    args = m_args;
    r = cct->_conf->parse_argv(args);
    if (r < 0) {
      derr << "could not parse command line args for " << description << ": "
	   << cpp_strerror(r) << dendl;
      cct->put();
      return r;
    }
  }

  // disable unnecessary librbd cache
  cct->_conf->set_val_or_die("rbd_cache", "false");
  cct->_conf->apply_changes(nullptr);
  cct->_conf->complain_about_parse_errors(cct);

  r = (*rados_ref)->init_with_context(cct);
  assert(r == 0);
  cct->put();

  r = (*rados_ref)->connect();
  if (r < 0) {
    derr << "error connecting to " << description << ": "
	 << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

void Replayer::run()
{
  dout(20) << "enter" << dendl;

  while (!m_stopping.read()) {
    std::string asok_hook_name = m_local_io_ctx.get_pool_name() + " " +
                                 m_peer.cluster_name;
    if (m_asok_hook_name != asok_hook_name || m_asok_hook == nullptr) {
      m_asok_hook_name = asok_hook_name;
      delete m_asok_hook;

      m_asok_hook = new ReplayerAdminSocketHook(g_ceph_context,
                                                m_asok_hook_name, this);
    }

    Mutex::Locker locker(m_lock);
    if (m_pool_watcher && m_pool_watcher->is_blacklisted()) {
      m_blacklisted = true;
      m_stopping.set(1);
      break;
    }

    m_cond.WaitInterval(m_lock, utime_t(1, 0));
  }
}

void Replayer::print_status(Formatter *f, stringstream *ss)
{
  dout(20) << "enter" << dendl;

  if (!f) {
    return;
  }

  Mutex::Locker l(m_lock);

  f->open_object_section("replayer_status");
  f->dump_string("pool", m_local_io_ctx.get_pool_name());
  f->dump_stream("peer") << m_peer;
  f->dump_string("instance_id", m_instance_watcher->get_instance_id());

  std::string leader_instance_id;
  m_leader_watcher->get_leader_instance_id(&leader_instance_id);
  f->dump_string("leader_instance_id", leader_instance_id);

  bool leader = m_leader_watcher->is_leader();
  f->dump_bool("leader", leader);
  if (leader) {
    std::vector<std::string> instance_ids;
    m_leader_watcher->list_instances(&instance_ids);
    f->open_array_section("instances");
    for (auto instance_id : instance_ids) {
      f->dump_string("instance_id", instance_id);
    }
    f->close_section();
  }

  m_instance_replayer->print_status(f, ss);

  f->close_section();
  f->flush(*ss);
}

void Replayer::start()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_instance_replayer->start();
}

void Replayer::stop(bool manual)
{
  dout(20) << "enter: manual=" << manual << dendl;

  Mutex::Locker l(m_lock);
  if (!manual) {
    m_stopping.set(1);
    m_cond.Signal();
    return;
  } else if (m_stopping.read()) {
    return;
  }

  m_instance_replayer->stop();
}

void Replayer::restart()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read()) {
    return;
  }

  m_instance_replayer->restart();
}

void Replayer::flush()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read() || m_manual_stop) {
    return;
  }

  m_instance_replayer->flush();
}

void Replayer::release_leader()
{
  dout(20) << "enter" << dendl;

  Mutex::Locker l(m_lock);

  if (m_stopping.read() || !m_leader_watcher) {
    return;
  }

  m_leader_watcher->release_leader();
}

void Replayer::handle_update(const std::string &mirror_uuid,
                             const ImageIds &added_image_ids,
                             const ImageIds &removed_image_ids) {
  assert(!mirror_uuid.empty());
  if (m_stopping.read()) {
    return;
  }

  dout(10) << dendl;
  Mutex::Locker locker(m_lock);
  if (!m_leader_watcher->is_leader()) {
    return;
  }

  if (m_peer.uuid != mirror_uuid) {
    m_instance_replayer->remove_peer(m_peer.uuid);
    m_instance_replayer->add_peer(mirror_uuid, m_remote_io_ctx);
    m_peer.uuid = mirror_uuid;
  }

  // first callback will be a full directory -- so see if we need to remove
  // any local images that no longer exist on the remote side
  if (!m_init_image_ids.empty()) {
    dout(20) << "scanning initial local image set" << dendl;
    for (auto &image_id : added_image_ids) {
      auto it = m_init_image_ids.find(image_id);
      if (it != m_init_image_ids.end()) {
        m_init_image_ids.erase(it);
      }
    }

    // the remaining images in m_init_image_ids must be deleted
    for (auto &image_id : m_init_image_ids) {
      dout(20) << "scheduling the deletion of init image: "
               << image_id.global_id << " (" << image_id.id << ")" << dendl;
      m_image_deleter->schedule_image_delete(m_local_rados, m_local_pool_id,
                                             image_id.id, image_id.global_id);
    }
    m_init_image_ids.clear();
  }

  m_update_op_tracker.start_op();
  Context *ctx = new FunctionContext([this](int r) {
      dout(20) << "complete handle_update: r=" << r << dendl;
      m_update_op_tracker.finish_op();
    });

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);

  for (auto &image_id : removed_image_ids) {
    // for now always send to myself (the leader)
    std::string &instance_id = m_instance_watcher->get_instance_id();
    m_instance_watcher->notify_image_release(instance_id, image_id.global_id,
                                             mirror_uuid, image_id.id, true,
                                             gather_ctx->new_sub());
  }

  for (auto &image_id : added_image_ids) {
    // for now always send to myself (the leader)
    std::string &instance_id = m_instance_watcher->get_instance_id();
    m_instance_watcher->notify_image_acquire(instance_id, image_id.global_id,
                                             mirror_uuid, image_id.id,
                                             gather_ctx->new_sub());
  }

  gather_ctx->activate();
}

void Replayer::handle_post_acquire_leader(Context *on_finish) {
  dout(20) << dendl;
  refresh_local_images(on_finish);
}

void Replayer::handle_pre_release_leader(Context *on_finish) {
  dout(20) << dendl;
  shut_down_pool_watcher(on_finish);
}

void Replayer::refresh_local_images(Context *on_finish) {
  dout(20) << dendl;

  // ensure the initial set of local images is up-to-date
  // after acquiring the leader role
  auto ctx = new C_RefreshLocalImages(this, on_finish);
  auto req = pool_watcher::RefreshImagesRequest<>::create(
    m_local_io_ctx, &ctx->image_ids, ctx);
  req->send();
}

void Replayer::handle_refresh_local_images(int r, ImageIds &&image_ids,
                                           Context *on_finish) {
  dout(20) << "r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_init_image_ids = std::move(image_ids);
  }

  if (r < 0) {
    derr << "failed to retrieve local images: " << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  init_pool_watcher(on_finish);
}

void Replayer::init_pool_watcher(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);
  assert(!m_pool_watcher);
  m_pool_watcher.reset(new PoolWatcher<>(
    m_threads, m_remote_io_ctx, m_pool_watcher_listener));
  m_pool_watcher->init(create_async_context_callback(
    m_threads->work_queue, on_finish));

  m_cond.Signal();
}

void Replayer::shut_down_pool_watcher(Context *on_finish) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_pool_watcher) {
      Context *ctx = new FunctionContext([this, on_finish](int r) {
          handle_shut_down_pool_watcher(r, on_finish);
      });
      ctx = create_async_context_callback(m_threads->work_queue, ctx);

      m_pool_watcher->shut_down(ctx);
      return;
    }
  }

  on_finish->complete(0);
}

void Replayer::handle_shut_down_pool_watcher(int r, Context *on_finish) {
  dout(20) << "r=" << r << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_pool_watcher);
    m_pool_watcher.reset();
  }
  wait_for_update_ops(on_finish);
}

void Replayer::wait_for_update_ops(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  Context *ctx = new FunctionContext([this, on_finish](int r) {
      handle_wait_for_update_ops(r, on_finish);
    });
  ctx = create_async_context_callback(m_threads->work_queue, ctx);

  m_update_op_tracker.wait_for_ops(ctx);
}

void Replayer::handle_wait_for_update_ops(int r, Context *on_finish) {
  dout(20) << "r=" << r << dendl;

  assert(r == 0);

  Mutex::Locker locker(m_lock);
  m_instance_replayer->release_all(on_finish);
}

} // namespace mirror
} // namespace rbd
