#define EUNJI
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013- Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_BUDDYSTORE_H
#define CEPH_BUDDYSTORE_H

#include <mutex>
#include <boost/intrusive_ptr.hpp>

#include "include/unordered_map.h"
#include "include/memory.h"
#include "include/Spinlock.h"
#include "common/Finisher.h"
#include "common/RefCountedObj.h"
#include "common/RWLock.h"
#include "os/ObjectStore.h"
#include "PageSet.h"
#include "include/assert.h"


#ifdef EUNJI
#include "BuddyHashIndexFile.h"
#include "buddy_types.h"
#include "BDJournalingObjectStore.h"
#include "BuddyLogger.h"
#endif

#define HOLD_IN_MEMORY

//// file io 
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <errno.h>
#include <dirent.h>
#include <sys/ioctl.h>

#if defined(__linux__)
#include <linux/fs.h>
#endif

#define dout_context cct

#if 0
enum {
  l_buddystore_first = 1000000,
  l_buddystore_journal_queue_ops,
  l_buddystore_journal_queue_bytes,
  l_buddystore_journal_ops,
  l_buddystore_journal_bytes,
  l_buddystore_journal_latency,
  l_buddystore_journal_wr,
  l_buddystore_journal_wr_bytes,
  l_buddystore_journal_full,
  l_buddystore_committing,
  l_buddystore_commitcycle,
  l_buddystore_commitcycle_interval,
  l_buddystore_commitcycle_latency,
  l_buddystore_op_queue_max_ops,
  l_buddystore_op_queue_ops,
  l_buddystore_ops,
  l_buddystore_op_queue_max_bytes,
  l_buddystore_op_queue_bytes,
  l_buddystore_bytes,
  l_buddystore_apply_latency,
  l_buddystore_queue_transaction_latency_avg,
  l_buddystore_last,
};
#endif
//class BuddyStore : public ObjectStore, public Test {
class BuddyStore : public BDJournalingObjectStore {
public:
  struct Object : public RefCountedObject {
    std::mutex xattr_mutex;
    std::mutex omap_mutex;
    map<string,bufferptr> xattr;
    bufferlist omap_header;
    map<string,bufferlist> omap;

    typedef boost::intrusive_ptr<Object> Ref;
    friend void intrusive_ptr_add_ref(Object *o) { o->get(); }
    friend void intrusive_ptr_release(Object *o) { o->put(); }

    Object() : RefCountedObject(nullptr, 0) {}
    // interface for object data
    virtual size_t get_size() const = 0;
    virtual int read(uint64_t offset, uint64_t len, bufferlist &bl) = 0;
    virtual int write(uint64_t offset, const bufferlist &bl) = 0;
    virtual int clone(Object *src, uint64_t srcoff, uint64_t len,
                      uint64_t dstoff) = 0;
    virtual int truncate(uint64_t offset) = 0;
    virtual void encode(bufferlist& bl) const = 0;
    virtual void decode(bufferlist::iterator& p) = 0;

    void encode_base(bufferlist& bl) const {
      ::encode(xattr, bl);
      ::encode(omap_header, bl);
      ::encode(omap, bl);
    }
    void decode_base(bufferlist::iterator& p) {
      ::decode(xattr, p);
      ::decode(omap_header, p);
      ::decode(omap, p);
    }

    void dump(Formatter *f) const {
      f->dump_int("data_len", get_size());
      f->dump_int("omap_header_len", omap_header.length());

      f->open_array_section("xattrs");
      for (map<string,bufferptr>::const_iterator p = xattr.begin();
	   p != xattr.end();
	   ++p) {
	f->open_object_section("xattr");
	f->dump_string("name", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();
      }
      f->close_section();

      f->open_array_section("omap");
      for (map<string,bufferlist>::const_iterator p = omap.begin();
	   p != omap.end();
	   ++p) {
	f->open_object_section("pair");
	f->dump_string("key", p->first);
	f->dump_int("length", p->second.length());
	f->close_section();
      }
      f->close_section();
    }
  };
  typedef Object::Ref ObjectRef;

  struct PageSetObject;

  struct Collection : public CollectionImpl {
    string basedir;
    coll_t cid;
    int bits;
    CephContext *cct;
#ifdef EUNJI
    BuddyHashIndexFile* data_file;
    int fd;
#endif
    bool use_page_set;
    ceph::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
    map<ghobject_t, ObjectRef> object_map;        ///< for iteration
    map<string,bufferptr> xattr;
    RWLock lock;   ///< for object_{map,hash}
    bool exists;

    typedef boost::intrusive_ptr<Collection> Ref;
    friend void intrusive_ptr_add_ref(Collection *c) { c->get(); }
    friend void intrusive_ptr_release(Collection *c) { c->put(); }

    const coll_t &get_cid() override {
      return cid;
    }

    ObjectRef create_object() const;

    // NOTE: The lock only needs to protect the object_map/hash, not the
    // contents of individual objects.  The osd is already sequencing
    // reads and writes, so we will never see them concurrently at this
    // level.

    ObjectRef get_object(ghobject_t oid) {
      RWLock::RLocker l(lock);
      auto o = object_hash.find(oid);
      if (o == object_hash.end())
	return ObjectRef();
      return o->second;
    }

    ObjectRef get_or_create_object(ghobject_t oid) {
      RWLock::WLocker l(lock);
      auto result = object_hash.emplace(oid, ObjectRef());
      if (result.second) // True 면 insertion 을 한것임. 즉, 원래 없었단 뜻. 
        object_map[oid] = result.first->second = create_object(); // 그럼 dnjsfo
      return result.first->second; // 있는 경우에는 그냥 있는거 return. 그걸 바꿔치기 한거니까. 
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(xattr, bl);
      ::encode(use_page_set, bl);
      uint32_t s = object_map.size();
      ::encode(s, bl);
      for (map<ghobject_t, ObjectRef>::const_iterator p = object_map.begin();
	   p != object_map.end();
	   ++p) {
	::encode(p->first, bl);
	p->second->encode(bl);
      }
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p) {
      DECODE_START(1, p);
      ::decode(xattr, p);
      ::decode(use_page_set, p);
      uint32_t s;
      ::decode(s, p);
      while (s--) {
	ghobject_t k;
	::decode(k, p);
	auto o = create_object();
	o->decode(p);
	object_map.insert(make_pair(k, o));
	object_hash.insert(make_pair(k, o));
      }
      DECODE_FINISH(p);
    }

    uint64_t used_bytes() const {
      uint64_t result = 0;
      for (map<ghobject_t, ObjectRef>::const_iterator p = object_map.begin();
	   p != object_map.end();
	   ++p) {
        result += p->second->get_size();
      }

      return result;
    }

    //explicit Collection(CephContext *cct, const string& basedir_, coll_t c, bool hdio)
    explicit Collection(CephContext *cct, const string& basedir_, coll_t c)
      : basedir(basedir_),
	cid(c),
	cct(cct),
	//data_file(cct, cid, basedir + "/" + cid.to_str() + ".data"),
	use_page_set(cct->_conf->buddystore_page_set),
        lock("BuddyStore::Collection::lock", true, false),
	exists(true) {}
	  //bd_coll = bdfs.buddy_create_file_object(cct, c);
//#endif
  };
  typedef Collection::Ref CollectionRef;

private:
  class OmapIteratorImpl;

#ifdef EUNJI
  string internal_name;
  string basedir, journalpath;
  osflagbits_t generic_flags;
  uuid_d fsid;
  PerfCounters *logger;

  bool data_directio;
  bool data_flush;
  bool data_sync;


  // -- op workqueue --
  struct Op {
    utime_t start;
    uint64_t op;
    vector<Transaction> tls;
    vector<buddy_iov_t> tls_iov;
    Context *onreadable, *onreadable_sync, *ondisk;
    uint64_t ops, bytes;
    TrackedOpRef osd_op;

    //
    atomic_t ref;
  };

  class OpSequencer : public Sequencer_impl {
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    //list<uint64_t> jq;
    list<Op*> jq;
    list<Op*> dq;
    list<pair<uint64_t, Context*> > flush_commit_waiters;

   // map<uint64_t, Context*> journal_commit_waiters; // journal 완료시 호출할 context 
    map<uint64_t,int> jcount; // seq, count

    uint64_t min_journal_commit = 0;

    Cond cond;
  public:
    Sequencer *parent;
    Mutex apply_lock;  // for apply mutual exclusion
    Mutex data_lock;
    int id;

    bool _get_max_uncompleted(
      uint64_t *seq ///< [out] max uncompleted seq
      );

    bool _get_min_uncompleted(
      uint64_t *seq ///< [out] min uncompleted seq
      );
  
    void _wake_flush_waiters(list<Context*> *to_queue);
  
    void queue_data(Op *o);
    Op *peek_queue_data();
    Op *dequeue_data(list<Context*> *to_queue);
 
    void queue_journal(Op *o);
    Op *peek_queue_journal();
    Op *dequeue_journal(list<Context*> *to_queue);
   
    bool get_max_uncompleted(
      uint64_t *seq ///< [out] min uncompleted seq
      );

    void set_jcount(uint64_t seq, int count);
    int dec_jcount(uint64_t seq); 

    void queue(Op *o);
    Op *peek_queue(); 
    Op *dequeue(list<Context*> *to_queue);

    void flush() override;
    bool flush_commit(Context *c) override;

      /******************
     * flush operations 
     * *****************/
#if 0
    bool _get_max_uncompleted(
      uint64_t *seq ///< [out] max uncompleted seq
      ) {
      assert(qlock.is_locked());
      assert(seq);
      *seq = 0;
      if (q.empty() && jq.empty())
	return true;

      if (!q.empty())
	*seq = q.back()->op;
      if (!jq.empty() && jq.back()->op > *seq)
	*seq = jq.back()->op;

      return false;
    } /// @returns true if both queues are empty

    bool _get_min_uncompleted(
      uint64_t *seq ///< [out] min uncompleted seq
      ) {
      assert(qlock.is_locked());
      assert(seq);
      *seq = 0;
      if (q.empty() && jq.empty())
	return true;

      if (!q.empty())
	*seq = q.front()->op;
      if (!jq.empty() && jq.front()->op < *seq)
	*seq = jq.front()->op;

      return false;
    } /// @returns true if both queues are empty


   void _wake_flush_waiters(list<Context*> *to_queue) {
    // seems no use in filestore.. 
 
      uint64_t seq;
      if (_get_min_uncompleted(&seq))
	seq = -1;

      for (list<pair<uint64_t, Context*> >::iterator i =
	     flush_commit_waiters.begin();
	   i != flush_commit_waiters.end() && i->first < seq;
	   flush_commit_waiters.erase(i++)) {
	to_queue->push_back(i->second);
      }
    }

   /*****************************
    * jq functions 
    * ********************/
#if 0
    void queue_journal(uint64_t s) {
      Mutex::Locker l(qlock);
      jq.push_back(s);
    }
    void dequeue_journal(list<Context*> *to_queue) {
      Mutex::Locker l(qlock);
      //assert(jcount[s] == 0);
      jq.pop_front();
      cond.Signal();
      _wake_flush_waiters(to_queue);
    }
#endif

    void queue_journal(Op *o) {
      Mutex::Locker l(qlock);
      jq.push_back(o);
    }
    Op *peek_queue_journal() {
      Mutex::Locker l(qlock);
      assert(data_lock.is_locked());
      return jq.front();
    }

    Op *dequeue_journal(list<Context*> *to_queue) {
      assert(to_queue);
//      assert(data_lock.is_locked());
      Mutex::Locker l(qlock);
      Op *o = jq.front();
      assert(jcount[o->op] == 0);
      jcount.erase(o->op);

      jq.pop_front();
      cond.Signal();

      _wake_flush_waiters(to_queue);
      return o;
    }

    bool get_max_uncompleted(
      uint64_t *seq ///< [out] min uncompleted seq
      ) {
      Mutex::Locker l(qlock);
      return _get_max_uncompleted(seq);
    } /// @returns true if both queues are empty


   /*****************************
    * jcount functions  
    * ********************/

    // jq 는 건드리지 않음. 
    // jq 말고 ioq 도 같이.. 두고 
    // journal 에서 완료되면 jq 에서는 빼버림. 
    // ioq 도 완료되면 빼버리기. 
    // context map 이 관리하면서 실제 ondisk 되는 시점 조절하기. 
    

    void set_jcount(uint64_t seq, int count){  
      Mutex::Locker l(qlock);
      jcount[seq] = count;  
    }

    //int dec_jcount(uint64_t seq, Context* c, list<Context*> to_queue){
    //int dec_jcount(uint64_t seq, Context* c){
    int dec_jcount(uint64_t seq){
      Mutex::Locker l(qlock);
      assert(jcount[seq] > 0);

      jcount[seq]--;

      return jcount[seq];
    }
#if 0
      // journal commit  
      if(jcount[seq] == 0 && seq == (last_oncommit + 1)) {
	  
	  // take away context upto max_journal_commit 
	  for (map<uint64_t, Context*>::iterator i =
	     journal_commit_waiters.begin();
	   i != journal_commit_waiters.end() && jcount[i->first] == 0;
	   journal_commit_waiters.erase(i++)) {
	    to_queue->push_back(i->second);
	    last_oncommit = i->first;
	  }
	  jcount.erase(seq);
	  return last_on_commit; // 이 seq 까지 dequeue 시키면 됨. 
      }

      // 내가 0이 아니거나 내가 처음 아니면.. 걍 매달고 나가야함. 
      auto result = journal_commit_waiters.insert(make_pair(seq, c));
      assert(result.second);

      return result.second;

      // 이 함수에서는 jq 는 건드리지 않기 때문에 dequeue 는 따로 해줘야 함. 
      // return 값이 0인 경우에만 dequeue_journal 해주어야 함. 
    }
#endif


   /*****************************
    *  q functions 
    * ********************/

    void queue(Op *o) {
      Mutex::Locker l(qlock);
      q.push_back(o);
    }
    Op *peek_queue() {
      Mutex::Locker l(qlock);
      assert(apply_lock.is_locked());
      return q.front();
    }

    Op *dequeue(list<Context*> *to_queue) {
      assert(to_queue);
      assert(apply_lock.is_locked());
      Mutex::Locker l(qlock);
      Op *o = q.front();
      q.pop_front();
      cond.Signal();

      _wake_flush_waiters(to_queue);
      return o;
    }

    void flush() override {
      Mutex::Locker l(qlock);

      while (cct->_conf->filestore_blackhole)
	cond.Wait(qlock);  // wait forever


      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
	seq = q.back()->op;
      if (!jq.empty() && jq.back()->op > seq)
	seq = jq.back()->op;

      if (seq) {
	// everything prior to our watermark to drain through either/both queues
	while ((!q.empty() && q.front()->op <= seq) ||
	       (!jq.empty() && jq.front()->op <= seq))
	  cond.Wait(qlock);
      }
    }
    bool flush_commit(Context *c) override {
      Mutex::Locker l(qlock);
      uint64_t seq = 0;
      // true 면, 안끝낸거 없다는 뜻. 다 끝냈어! 
      if (_get_max_uncompleted(&seq)) {
	return true;
      } else {
	flush_commit_waiters.push_back(make_pair(seq, c));
	return false;
      }
    }
#endif

    OpSequencer(CephContext* cct, int i)
      : Sequencer_impl(cct),
	qlock("BuddyStore::OpSequencer::qlock", false, false),
	parent(0),
	apply_lock("BuddyStore::OpSequencer::apply_lock", false, false),
	data_lock("BuddyStore::OpSequencer::data_lock", false, false),
        id(i) {}
    ~OpSequencer() override {
      assert(q.empty());
    }

    const string& get_name() const {
      return parent->get_name();
    }
  }; // end of sequencer 

  friend ostream& operator<<(ostream& out, const OpSequencer& s);

  BuddyStore::Op *build_op(vector<Transaction>& tls,
				   Context *onreadable,
				   Context *onreadable_sync,
				   Context *ondisk,
				   TrackedOpRef osd_op);


  /************************************
   * journaling, data_write, do_transaction(parallel)
   *
   * **********************************/


  ////////////////////////////////////////////////////
  // data write  
  ////////////////////////////////////////////////////
  void generate_iov_data_bl(vector<buddy_iov_t>& iov, bufferlist& bl, uint32_t start_off);
  int generate_iov(vector<Transaction> &tls, vector<buddy_iov_t>& iov);

#if 0
  // data flush writer 
  class DataWriteThread : public Thread {
    BuddyStore *bs;
  public:
    explicit DataWriteThread(BuddyStore *bs_) : bs(bs_) {}
    void *entry() override {
      bs->data_write_thread_entry();
      return 0;
    }
  } data_write_thread;

  void start_data_write_thread();
  void stop_data_write_thread();

  void data_write_thread_entry();

  Mutex data_writeq_lock;
  Cond data_writeq_cond;
  list<Op*> data_writeq;
  bool data_writeq_empty();
#endif
#if 0
  Op &peek_data_write();
  void pop_data_write();
//  void batch_pop_write(list<write_item> &items);
//  void batch_unpop_write(list<write_item> &items);

#endif
  bool data_write_stop;

  ////////////////////////////////////////////////////
  // Journaling 
  ////////////////////////////////////////////////////
  
  uint64_t last_checkpointed_seq;
  atomic_t next_osr_id;


//  Mutex journal_finish_lock;
//  Cond journal_finish_cond;
  Cond sync_cond;

  bool m_journal_dio, m_journal_aio, m_journal_force_aio;


  int _do_transactions(
    vector<Transaction> &tls, uint64_t op_seq,
    ThreadPool::TPHandle *handle);

  int do_transactions(vector<Transaction> &tls, uint64_t op_seq) override {
    return 0;
    // 이거는 journal_replay 손볼때 같이 고쳐야 함. 
    //return _do_transactions(tls, op_seq, 0);
  }

  // C_JournalCompletion -> _finish_journal -> ondisk 
  void _finish_journal(OpSequencer *osr, Op *o, Context *ondisk);
  friend struct C_JournalCompletion;

  void new_journal();
  
  void dump_logger();

  
  //void dump_perf_counters(Formatter *f) override {

  void dump_perf_counters(Formatter *f) {
    f->open_object_section("perf_counters");
    if(logger)
      logger->dump_formatted(f, false);
    f->close_section();
  }


  ////////////////////////////////////////////////////
  // do_transaction  
  // 참고로.. vector io 는 따로 할 것임. 
  // in-memory data structure 에 반영함. 
  //
  ////////////////////////////////////////////////////
  
  deque<OpSequencer*> op_queue;

  ThreadPool op_tp;

  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {

    BuddyStore *store;
    OpWQ(BuddyStore *fs, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<OpSequencer>("BuddyStore::OpWQ", timeout, suicide_timeout, tp), store(fs) {}

    bool _enqueue(OpSequencer *osr) override {
      store->op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) override {
      ceph_abort();
    }
    bool _empty() override {
      return store->op_queue.empty();
    }
    OpSequencer *_dequeue() override {
      if (store->op_queue.empty())
	return NULL;
      OpSequencer *osr = store->op_queue.front();
      store->op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) override {
      store->_do_op(osr, handle);
    }
    void _process_finish(OpSequencer *osr) override {
      store->_finish_op(osr);
    }
    void _clear() override {
      assert(store->op_queue.empty());
    }
  } op_wq;

  void _do_op(OpSequencer *o, ThreadPool::TPHandle &handle);
  void _finish_op(OpSequencer *o);
  void queue_op(OpSequencer *osr, Op *o);
  void op_queue_reserve_throttle(Op *o) {}
  void op_queue_release_throttle(Op *o) {}



  ////////////////////////////////////////////////////
  // vector io workqueue 
  //
  ////////////////////////////////////////////////////
  
  deque<OpSequencer*> data_op_queue;

  ThreadPool data_op_tp;

  struct DataOpWQ : public ThreadPool::WorkQueue<OpSequencer> {

    BuddyStore *store;
    DataOpWQ(BuddyStore *fs, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<OpSequencer>("BuddyStore::DataOpWQ", timeout, suicide_timeout, tp), store(fs) {}

    bool _enqueue(OpSequencer *osr) override {
      store->data_op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) override {
      ceph_abort();
    }
    bool _empty() override {
      return store->data_op_queue.empty();
    }
    OpSequencer *_dequeue() override {
      if (store->data_op_queue.empty())
	return NULL;
      OpSequencer *osr = store->data_op_queue.front();
      store->data_op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) override {
      store->_do_data_op(osr, handle);
    }
    void _process_finish(OpSequencer *osr) override {
      store->_finish_data_op(osr);
    }
    void _clear() override {
      assert(store->data_op_queue.empty());
    }
  } data_op_wq;

  void _do_data_op(OpSequencer *o, ThreadPool::TPHandle &handle);
  void _finish_data_op(OpSequencer *o);
  void queue_data_op(OpSequencer *osr, Op *o);
  void data_op_queue_reserve_throttle(Op *o) {}
  void data_op_queue_release_throttle(Op *o) {}


#endif // EUNJI 

  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map
  // EUNJI 
  ceph::unordered_map<coll_t, BuddyHashIndexFile*> coll_file_map;
  RWLock coll_file_lock;

  CollectionRef get_collection(const coll_t& cid);

  Finisher ondisk_finisher; /// ==> 이게 문제였구먼..
  Finisher apply_finisher;

  Mutex ondisk_finisher_lock; // 이것도 문제였구먼. lock 안잡고 서로 다른 thread 가 달려들어 queue 하면 문제생김. 

  uint64_t used_bytes;

  void _do_transaction(Transaction& t, uint64_t op_Seq, int trans_num,
      ThreadPool::TPHandle *handle);
  //void _do_transaction(Transaction& t);

  int _touch(const coll_t& cid, const ghobject_t& oid);
  int _write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len,
	      const bufferlist& bl, uint32_t fadvise_flags = 0);

  int _zero(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size);
  int _remove(const coll_t& cid, const ghobject_t& oid);
  int _setattrs(const coll_t& cid, const ghobject_t& oid, map<string,bufferptr>& aset);
  int _rmattr(const coll_t& cid, const ghobject_t& oid, const char *name);
  int _rmattrs(const coll_t& cid, const ghobject_t& oid);
  int _clone(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid);
  int _clone_range(const coll_t& cid, const ghobject_t& oldoid,
		   const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _omap_clear(const coll_t& cid, const ghobject_t &oid);
  int _omap_setkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& aset_bl);
  int _omap_rmkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& keys_bl);
  int _omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
		       const string& first, const string& last);
  int _omap_setheader(const coll_t& cid, const ghobject_t &oid, const bufferlist &bl);

  int _collection_hint_expected_num_objs(const coll_t& cid, uint32_t pg_num,
      uint64_t num_objs) const { return 0; }
  int _create_collection(const coll_t& c, int bits);
  int _destroy_collection(const coll_t& c);
  int _collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid);
  int _collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
			      coll_t cid, const ghobject_t& o);
  int _split_collection(const coll_t& cid, uint32_t bits, uint32_t rem, coll_t dest);

  int _save();
  int _load();

  void dump(Formatter *f);
  void dump_all();

public:
  BuddyStore(CephContext *cct, const string& basedir_, osflagbits_t flags = 0,
      const char *internal_name = "buddystore");
  ~BuddyStore() override;


//      mem_op_tp(cct, "BuddyStore::mem_op_tp", "tp_bdstore_mem_op", cct->_conf->filestore_op_threads, "filestore_op_threads"),
//      mem_op_wq(this, cct->_conf->filestore_op_thread_timeout,
//	cct->_conf->filestore_op_thread_suicide_timeout, &mem_op_tp),
//      data_write_stop(true)



  string get_type() override {
//    return "memstore";
    return "buddystore";
	
  }

  bool test_mount_in_use() override {
    return false;
  }

  int mount() override;
  int umount() override;

  int fsck(bool deep) override {
    return 0;
  }

  int validate_hobject_key(const hobject_t &obj) const override {
    return 0;
  }
  unsigned get_max_attr_name_length() override {
    return 256;  // arbitrary; there is no real limit internally
  }

  int mkfs() override;

#ifdef EUNJI
  int mkjournal() override;
  bool wants_journal() override {
    return true;
  }
  bool allows_journal() override {
    return true;
  }
  bool needs_journal() override {
    return false;
  }
#else
  int mkjournal() override {
    return 0;
  }
  bool wants_journal() override {
    return false;
  }
  bool allows_journal() override {
    return false;
  }
  bool needs_journal() override {
    return false;
  }
#endif
  int statfs(struct store_statfs_t *buf) override;

  bool exists(const coll_t& cid, const ghobject_t& oid) override;
  bool exists(CollectionHandle &c, const ghobject_t& oid) override;
  int stat(const coll_t& cid, const ghobject_t& oid,
	   struct stat *st, bool allow_eio = false) override;
  int stat(CollectionHandle &c, const ghobject_t& oid,
	   struct stat *st, bool allow_eio = false) override;
  int set_collection_opts(
    const coll_t& cid,
    const pool_opts_t& opts) override;
  int read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false) override;
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false) override;
  using ObjectStore::fiemap;
  int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl) override;
  int fiemap(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap) override;
  int getattr(const coll_t& cid, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;
  int getattr(CollectionHandle &c, const ghobject_t& oid, const char *name,
	      bufferptr& value) override;
  int getattrs(const coll_t& cid, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;
  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       map<string,bufferptr>& aset) override;

  int list_collections(vector<coll_t>& ls) override;

  CollectionHandle open_collection(const coll_t& c) override {
    return get_collection(c);
  }
  bool collection_exists(const coll_t& c) override;
  int collection_empty(const coll_t& c, bool *empty) override;
  int collection_bits(const coll_t& c) override;
  using ObjectStore::collection_list;
  int collection_list(const coll_t& cid,
		      const ghobject_t& start, const ghobject_t& end, int max,
		      vector<ghobject_t> *ls, ghobject_t *next) override;

  using ObjectStore::omap_get;
  int omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) override;

  using ObjectStore::omap_get_header;
  /// Get omap header
  int omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) override;

  using ObjectStore::omap_get_keys;
  /// Get keys defined on oid
  int omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    ) override;

  using ObjectStore::omap_get_values;
  /// Get key values
  int omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) override;

  using ObjectStore::omap_check_keys;
  /// Filters keys into out which are defined on oid
  int omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    ) override;

  using ObjectStore::get_omap_iterator;
  ObjectMap::ObjectMapIterator get_omap_iterator(
    const coll_t& cid,              ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    ) override;

  void set_fsid(uuid_d u) override;
  uuid_d get_fsid() override;

  uint64_t estimate_objects_overhead(uint64_t num_objects) override {
    return 0; //do not care
  }

  objectstore_perf_stat_t get_cur_stats() override;

  const PerfCounters* get_perf_counters() const override {
    return nullptr;
  }


  int queue_transactions(
    Sequencer *osr, vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) override;
};

#endif
