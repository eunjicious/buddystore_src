// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Replay.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/EventPreprocessor.h"
#include "tools/rbd_mirror/ImageSyncThrottler.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"

namespace librbd {

namespace {

struct MockTestJournal;

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
  MockTestJournal *journal = nullptr;
};

struct MockTestJournal : public MockJournal {
  MOCK_METHOD2(start_external_replay, void(journal::Replay<MockTestImageCtx> **,
                                           Context *on_start));
  MOCK_METHOD0(stop_external_replay, void());
};

} // anonymous namespace

namespace journal {

template<>
struct Replay<MockTestImageCtx> {
  MOCK_METHOD2(decode, int(bufferlist::iterator *, EventEntry *));
  MOCK_METHOD3(process, void(const EventEntry &, Context *, Context *));
  MOCK_METHOD1(flush, void(Context*));
  MOCK_METHOD2(shut_down, void(bool, Context*));
};

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef ::journal::MockJournalerProxy Journaler;
  typedef ::journal::MockReplayEntryProxy ReplayEntry;
};

struct MirrorPeerClientMeta;

} // namespace journal
} // namespace librbd

namespace rbd {
namespace mirror {

template<>
class ImageSync<librbd::MockTestImageCtx> {
public:
  static ImageSync* create(librbd::MockTestImageCtx *local_image_ctx,
                           librbd::MockTestImageCtx *remote_image_ctx,
                           SafeTimer *timer, Mutex *timer_lock,
                           const std::string &mirror_uuid,
                           journal::MockJournaler *journaler,
                           librbd::journal::MirrorPeerClientMeta *client_meta,
                           ContextWQ *work_queue, Context *on_finish,
                           ProgressContext *progress_ctx = nullptr) {
    assert(0 == "unexpected call");
    return nullptr;
  }

  void send() {
  }
};

namespace image_replayer {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::MatcherCast;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::WithArg;

template<>
struct BootstrapRequest<librbd::MockTestImageCtx> {
  static BootstrapRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;
  bool *do_resync = nullptr;

  static BootstrapRequest* create(librados::IoCtx &local_io_ctx,
        librados::IoCtx &remote_io_ctx,
        rbd::mirror::ImageSyncThrottlerRef<librbd::MockTestImageCtx> image_sync_throttler,
        librbd::MockTestImageCtx **local_image_ctx,
        const std::string &local_image_name,
        const std::string &remote_image_id,
        const std::string &global_image_id,
        ContextWQ *work_queue, SafeTimer *timer,
        Mutex *timer_lock,
        const std::string &local_mirror_uuid,
        const std::string &remote_mirror_uuid,
        ::journal::MockJournalerProxy *journaler,
        librbd::journal::MirrorPeerClientMeta *client_meta,
        Context *on_finish,
        bool *do_resync,
        rbd::mirror::ProgressContext *progress_ctx = nullptr) {
    assert(s_instance != nullptr);
    s_instance->image_ctx = local_image_ctx;
    s_instance->on_finish = on_finish;
    s_instance->do_resync = do_resync;
    return s_instance;
  }

  BootstrapRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  ~BootstrapRequest() {
    assert(s_instance == this);
    s_instance = nullptr;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(send, void());
  MOCK_METHOD0(cancel, void());
};

template<>
struct CloseImageRequest<librbd::MockTestImageCtx> {
  static CloseImageRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockTestImageCtx **image_ctx,
                                   Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->image_ctx = image_ctx;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CloseImageRequest() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  ~CloseImageRequest() {
    assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct EventPreprocessor<librbd::MockTestImageCtx> {
  static EventPreprocessor *s_instance;

  static EventPreprocessor *create(librbd::MockTestImageCtx &local_image_ctx,
                                   ::journal::MockJournalerProxy &remote_journaler,
                                   const std::string &local_mirror_uuid,
                                   librbd::journal::MirrorPeerClientMeta *client_meta,
                                   ContextWQ *work_queue) {
    assert(s_instance != nullptr);
    return s_instance;
  }

  static void destroy(EventPreprocessor* processor) {
  }

  EventPreprocessor() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  ~EventPreprocessor() {
    assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD1(is_required, bool(const librbd::journal::EventEntry &));
  MOCK_METHOD2(preprocess, void(librbd::journal::EventEntry *, Context *));
};

template<>
struct ReplayStatusFormatter<librbd::MockTestImageCtx> {
  static ReplayStatusFormatter* s_instance;

  static ReplayStatusFormatter* create(::journal::MockJournalerProxy *journaler,
                                       const std::string &mirror_uuid) {
    assert(s_instance != nullptr);
    return s_instance;
  }

  static void destroy(ReplayStatusFormatter* formatter) {
  }

  ReplayStatusFormatter() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  ~ReplayStatusFormatter() {
    assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD2(get_or_send_update, bool(std::string *description, Context *on_finish));
};

BootstrapRequest<librbd::MockTestImageCtx>* BootstrapRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
CloseImageRequest<librbd::MockTestImageCtx>* CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
EventPreprocessor<librbd::MockTestImageCtx>* EventPreprocessor<librbd::MockTestImageCtx>::s_instance = nullptr;
ReplayStatusFormatter<librbd::MockTestImageCtx>* ReplayStatusFormatter<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/ImageReplayer.cc"
#include "tools/rbd_mirror/ImageSyncThrottler.cc"

namespace rbd {
namespace mirror {

class TestMockImageReplayer : public TestMockFixture {
public:
  typedef BootstrapRequest<librbd::MockTestImageCtx> MockBootstrapRequest;
  typedef CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;
  typedef EventPreprocessor<librbd::MockTestImageCtx> MockEventPreprocessor;
  typedef ReplayStatusFormatter<librbd::MockTestImageCtx> MockReplayStatusFormatter;
  typedef librbd::journal::Replay<librbd::MockTestImageCtx> MockReplay;
  typedef ImageReplayer<librbd::MockTestImageCtx> MockImageReplayer;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    m_image_deleter.reset(new rbd::mirror::ImageDeleter(m_threads->work_queue,
                                                        m_threads->timer,
                                                        &m_threads->timer_lock));
    m_image_sync_throttler.reset(
      new rbd::mirror::ImageSyncThrottler<librbd::MockTestImageCtx>());

    m_image_replayer = new MockImageReplayer(
      m_threads, m_image_deleter, m_image_sync_throttler,
      rbd::mirror::RadosRef(new librados::Rados(m_local_io_ctx)),
      "local_mirror_uuid", m_local_io_ctx.get_id(), "global image id");
    m_image_replayer->add_remote_image(
      "remote_mirror_uuid", m_remote_image_ctx->id, m_remote_io_ctx);
  }

  void TearDown() override {
    delete m_image_replayer;

    TestMockFixture::TearDown();
  }

  void create_local_image() {
    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  bufferlist encode_tag_data(const librbd::journal::TagData &tag_data) {
    bufferlist bl;
    ::encode(tag_data, bl);
    return bl;
  }

  void expect_get_or_send_update(
    MockReplayStatusFormatter &mock_replay_status_formatter) {
    EXPECT_CALL(mock_replay_status_formatter, get_or_send_update(_, _))
      .WillRepeatedly(DoAll(WithArg<1>(CompleteContext(-EEXIST)),
                            Return(true)));
  }

  void expect_send(MockBootstrapRequest &mock_bootstrap_request,
                   librbd::MockTestImageCtx &mock_local_image_ctx,
                   bool do_resync, int r) {
    EXPECT_CALL(mock_bootstrap_request, send())
      .WillOnce(Invoke([&mock_bootstrap_request, &mock_local_image_ctx,
                        do_resync, r]() {
            if (r == 0) {
              *mock_bootstrap_request.image_ctx = &mock_local_image_ctx;
              *mock_bootstrap_request.do_resync = do_resync;
            }
            mock_bootstrap_request.on_finish->complete(r);
          }));
  }

  void expect_start_external_replay(librbd::MockTestJournal &mock_journal,
                                    MockReplay *mock_replay, int r) {
    EXPECT_CALL(mock_journal, start_external_replay(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mock_replay),
                      WithArg<1>(CompleteContext(r))));
  }

  void expect_init(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, init(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_get_cached_client(::journal::MockJournaler &mock_journaler,
                                int r) {
    librbd::journal::ImageClientMeta image_client_meta;
    image_client_meta.tag_class = 0;

    librbd::journal::ClientData client_data;
    client_data.client_meta = image_client_meta;

    cls::journal::Client client;
    ::encode(client_data, client.data);

    EXPECT_CALL(mock_journaler, get_cached_client("local_mirror_uuid", _))
      .WillOnce(DoAll(SetArgPointee<1>(client),
                      Return(r)));
  }

  void expect_stop_replay(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, stop_replay(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_shut_down(MockReplay &mock_replay, bool cancel_ops, int r) {
    EXPECT_CALL(mock_replay, shut_down(cancel_ops, _))
      .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  void expect_shut_down(journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, shut_down(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_send(MockCloseImageRequest &mock_close_image_request, int r) {
    EXPECT_CALL(mock_close_image_request, send())
      .WillOnce(Invoke([&mock_close_image_request, r]() {
            *mock_close_image_request.image_ctx = nullptr;
            mock_close_image_request.on_finish->complete(r);
          }));
  }

  void expect_get_commit_tid_in_debug(
    ::journal::MockReplayEntry &mock_replay_entry) {
    // It is used in debug messages and depends on debug level
    EXPECT_CALL(mock_replay_entry, get_commit_tid())
    .Times(AtLeast(0))
      .WillRepeatedly(Return(0));
  }

  void expect_committed(::journal::MockJournaler &mock_journaler, int times) {
    EXPECT_CALL(mock_journaler, committed(
                  MatcherCast<const ::journal::MockReplayEntryProxy&>(_)))
      .Times(times);
  }

  void expect_try_pop_front(::journal::MockJournaler &mock_journaler,
                            uint64_t replay_tag_tid, bool entries_available) {
    EXPECT_CALL(mock_journaler, try_pop_front(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(::journal::MockReplayEntryProxy()),
                      SetArgPointee<1>(replay_tag_tid),
                      Return(entries_available)));
  }

  void expect_try_pop_front_return_no_entries(
    ::journal::MockJournaler &mock_journaler, Context *on_finish) {
    EXPECT_CALL(mock_journaler, try_pop_front(_, _))
      .WillOnce(DoAll(Invoke([on_finish](::journal::MockReplayEntryProxy *e,
					 uint64_t *t) {
			       on_finish->complete(0);
			     }),
		      Return(false)));
  }

  void expect_get_tag(::journal::MockJournaler &mock_journaler,
                      const cls::journal::Tag &tag, int r) {
    EXPECT_CALL(mock_journaler, get_tag(_, _, _))
      .WillOnce(DoAll(SetArgPointee<1>(tag),
                      WithArg<2>(CompleteContext(r))));
  }

  void expect_allocate_tag(librbd::MockTestJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, allocate_tag(_, _, _))
      .WillOnce(WithArg<2>(CompleteContext(r)));
  }

  void expect_preprocess(MockEventPreprocessor &mock_event_preprocessor,
                         bool required, int r) {
    EXPECT_CALL(mock_event_preprocessor, is_required(_))
      .WillOnce(Return(required));
    if (required) {
      EXPECT_CALL(mock_event_preprocessor, preprocess(_, _))
        .WillOnce(WithArg<1>(CompleteContext(r)));
    }
  }

  void expect_process(MockReplay &mock_replay,
                      int on_ready_r, int on_commit_r) {
    EXPECT_CALL(mock_replay, process(_, _, _))
      .WillOnce(DoAll(WithArg<1>(CompleteContext(on_ready_r)),
                      WithArg<2>(CompleteContext(on_commit_r))));
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx = nullptr;
  std::shared_ptr<rbd::mirror::ImageDeleter> m_image_deleter;
  std::shared_ptr<rbd::mirror::ImageSyncThrottler<librbd::MockTestImageCtx>> m_image_sync_throttler;
  MockImageReplayer *m_image_replayer;
};

TEST_F(TestMockImageReplayer, StartStop) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // STOP

  MockCloseImageRequest mock_close_local_image_request;

  expect_stop_replay(mock_remote_journaler, 0);
  expect_shut_down(mock_local_replay, true, 0);

  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  expect_send(mock_close_local_image_request, 0);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}

TEST_F(TestMockImageReplayer, BootstrapError) {

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, -EINVAL);

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, StartExternalReplayError) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, nullptr, -EINVAL);

  MockCloseImageRequest mock_close_local_image_request;

  EXPECT_CALL(mock_local_journal, remove_listener(_));

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  expect_send(mock_close_local_image_request, 0);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, StopError) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // STOP (errors are ignored)

  MockCloseImageRequest mock_close_local_image_request;

  expect_stop_replay(mock_remote_journaler, -EINVAL);
  expect_shut_down(mock_local_replay, true, -EINVAL);

  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, -EINVAL);

  expect_send(mock_close_local_image_request, -EINVAL);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}

TEST_F(TestMockImageReplayer, Replay) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  ::journal::MockReplayEntry mock_replay_entry;

  expect_get_or_send_update(mock_replay_status_formatter);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_committed(mock_remote_journaler, 2);

  InSequence seq;
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // REPLAY

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_replay, false, 0);
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_replay, 0, 0);

  // the next event with preprocess
  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, true, 0);
  expect_process(mock_local_replay, 0, 0);

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);

  // fire
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, replay_ctx.wait());

  // STOP

  MockCloseImageRequest mock_close_local_image_request;

  expect_stop_replay(mock_remote_journaler, 0);
  expect_shut_down(mock_local_replay, true, 0);

  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  expect_send(mock_close_local_image_request, 0);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}

TEST_F(TestMockImageReplayer, DecodeError) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  ::journal::MockReplayEntry mock_replay_entry;

  expect_get_or_send_update(mock_replay_status_formatter);
  expect_get_commit_tid_in_debug(mock_replay_entry);

  InSequence seq;
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // REPLAY

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_replay, false, 0);
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(Return(-EINVAL));

  // stop on error
  expect_stop_replay(mock_remote_journaler, 0);
  expect_shut_down(mock_local_replay, true, 0);

  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  MockCloseImageRequest mock_close_local_image_request;
  C_SaferCond close_ctx;
  EXPECT_CALL(mock_close_local_image_request, send())
    .WillOnce(Invoke([&mock_close_local_image_request, &close_ctx]() {
	  *mock_close_local_image_request.image_ctx = nullptr;
	  mock_close_local_image_request.on_finish->complete(0);
	  close_ctx.complete(0);
	}));

  // fire
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, close_ctx.wait());

  while (!m_image_replayer->is_stopped()) {
    usleep(1000);
  }
}

TEST_F(TestMockImageReplayer, DelayedReplay) {

  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  ::journal::MockReplayEntry mock_replay_entry;

  expect_get_or_send_update(mock_replay_status_formatter);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_committed(mock_remote_journaler, 1);

  InSequence seq;
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // REPLAY

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_replay, false, 0);
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process with delay
  EXPECT_CALL(mock_replay_entry, get_data());
  librbd::journal::EventEntry event_entry(
    librbd::journal::AioDiscardEvent(123, 345, false), ceph_clock_now());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(DoAll(SetArgPointee<1>(event_entry),
                    Return(0)));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_replay, 0, 0);

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);

  // fire
  mock_local_image_ctx.mirroring_replay_delay = 2;
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, replay_ctx.wait());

  // add a pending (delayed) entry before stop
  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  EXPECT_CALL(mock_replay_entry, get_data());
  C_SaferCond decode_ctx;
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(DoAll(Invoke([&decode_ctx](bufferlist::iterator* it,
                                         librbd::journal::EventEntry *e) {
                             decode_ctx.complete(0);
                           }),
                    Return(0)));

  mock_local_image_ctx.mirroring_replay_delay = 10;
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, decode_ctx.wait());

  // STOP

  MockCloseImageRequest mock_close_local_image_request;

  expect_stop_replay(mock_remote_journaler, 0);
  expect_shut_down(mock_local_replay, true, 0);

  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  expect_send(mock_close_local_image_request, 0);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}

} // namespace mirror
} // namespace rbd
