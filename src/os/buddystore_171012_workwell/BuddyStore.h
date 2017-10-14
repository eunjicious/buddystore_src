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
//#include "BuddyDataFileObject.h"
#include "../filestore/DBObjectMap.h"
#include "../filestore/SequencerPosition.h"
#include "kv/KeyValueDB.h"
#include "BuddyLogDataFileObject.h"
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

    bool data_hold_in_memory;

    typedef boost::intrusive_ptr<Object> Ref;
    friend void intrusive_ptr_add_ref(Object *o) { o->get(); }
    friend void intrusive_ptr_release(Object *o) { o->put(); }

    Object(bool data_hold =true) : RefCountedObject(nullptr, 0), data_hold_in_memory(data_hold){}
    //Object() : RefCountedObject(nullptr, 0) {}
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

    // -- data_file_map --
    // 사실 이게.. buddy_index_map_t 를 Object 에 넣으면 되는건데
    map<ghobject_t, buddy_index_map_t> data_file_index_map; // data_file_index_map  

    int data_file_insert_index(const ghobject_t& oid, const off_t ooff, const off_t foff, const ssize_t bytes); 
    int data_file_get_index(const ghobject_t& oid, const off_t ooff, const ssize_t bytes, vector<buddy_iov_t>& iov);

    bool use_page_set;
    bool data_hold_in_memory;
    ceph::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
    map<ghobject_t, ObjectRef> object_map;        ///< for iteration
    map<string,bufferptr> xattr;
    RWLock c_lock;   ///< for object_{map,hash}
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
      RWLock::RLocker l(c_lock);
      auto o = object_hash.find(oid);
      if (o == object_hash.end())
	return ObjectRef();
      return o->second;
    }

    ObjectRef get_or_create_object(ghobject_t oid) {
      RWLock::WLocker l(c_lock);
      auto result = object_hash.emplace(oid, ObjectRef());
      if (result.second) // True 면 insertion 을 한것임. 즉, 원래 없었단 뜻. 
        object_map[oid] = result.first->second = create_object(); // 그럼 dnjsfo
      return result.first->second; // 있는 경우에는 그냥 있는거 return. 그걸 바꿔치기 한거니까. 
    }


    void encode_index(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(data_file_index_map, bl);
      ENCODE_FINISH(bl);
    }
    void decode_index(bufferlist::iterator& p) {
      DECODE_START(1, p);
      ::decode(data_file_index_map, p);
      DECODE_FINISH(p);
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
      // Eunji: here.. it's doing ..
      ::encode(data_file_index_map, bl);
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
      ::decode(data_file_index_map, p);
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

    explicit Collection(CephContext *cct, const string& basedir_, coll_t c)
      : basedir(basedir_),
	cid(c),
	cct(cct),
	//data_file(cct, cid, basedir + "/" + cid.to_str() + ".data"),
	use_page_set(cct->_conf->buddystore_page_set),
	data_hold_in_memory(cct->_conf->buddystore_data_hold_in_memory),
        c_lock("BuddyStore::Collection::c_lock", true, false),
	exists(true) {}
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


  // -- data file --
  bool data_directio;
  bool data_flush;
  bool data_sync;
  bool data_hold_in_memory;
  bool file_prewrite;
  bool file_inplace_write;

  uint64_t last_data_file_seq;

  BuddyLogDataFileObject data_file;

  bool debug_file_read;

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

  public:
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    //list<uint64_t> jq;
    //list<Op*> jq;
    list<Op*> dq; // protected by qlock 
    list<pair<uint64_t, Context*> > flush_commit_waiters;

   // map<uint64_t, Context*> journal_commit_waiters; // journal 완료시 호출할 context 
    map<uint64_t,int> jcount; // seq, count

    uint64_t min_journal_commit = 0;

    Cond cond;


    Sequencer *parent;
    Mutex apply_lock;  // for apply mutual exclusion

    int id;

    bool _get_max_uncompleted(
      uint64_t *seq ///< [out] max uncompleted seq
      );

    bool _get_min_uncompleted(
      uint64_t *seq ///< [out] min uncompleted seq
      );
  
    void _wake_flush_waiters(list<Context*> *to_queue);
  
    int get_data_num(){
      assert(qlock.is_locked());
      return dq.size();
    }
    void queue_data(Op *o);
    void batch_pop_queue_data(list<Op*>& ops);
    Op* pop_queue_data();

    //Op *peek_queue_data();
//    Op *dequeue_data(list<Context*> *to_queue);
 
//    void queue_journal(Op *o);
//    Op *peek_queue_journal();
//    Op *dequeue_journal(list<Context*> *to_queue);
  
    void dequeue_wait_ondisk(list<Context*> *to_queue); 


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
    OpSequencer(CephContext* cct, int i)
      : Sequencer_impl(cct),
	qlock("BuddyStore::OpSequencer::qlock", false, false),
	parent(0),
	apply_lock("BuddyStore::OpSequencer::apply_lock", false, false),
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

  void generate_iov_data_bl(vector<buddy_iov_t>& iov, bufferlist& bl, uint32_t start_off);
  int generate_iov(vector<Transaction> &tls, vector<buddy_iov_t>& iov);

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


  //int do_checkpoint();


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

  Mutex op_wq_lock;

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

  ////////////////////////////////////////////////////
  //
  // home_write_thread 
  //
  ////////////////////////////////////////////////////
#if 0 
  struct data_item {
    OpSequencer *osr;
    Context *finish;
    utime_t start;

    data_item(OpSequencer* _osr, Context *c, utime_t s)
      : osr(_osr), finish(c), start(s) {}
    data_item() : osr(0), finish(0), start(0) {}
  };
#endif
  Mutex dataq_lock;
  Cond dataq_cond;
  list<OpSequencer*> dataq;


  bool dataq_empty() {
    Mutex::Locker l(dataq_lock);
    return dataq.empty();
  }
  
  void queue_dataq(OpSequencer* osr) {
    Mutex::Locker l(dataq_lock);
    dataq.push_back(osr);
  }

  OpSequencer* &peek_dataq(){ // front 
    Mutex::Locker l(dataq_lock);
    assert(!dataq.empty());
    return dataq.front();
  }

  void pop_dataq(){ // front 
    Mutex::Locker l(dataq_lock);
    assert(!dataq.empty());
    dataq.pop_front();
  }
  void batch_pop_dataq(list<OpSequencer*> &items){// 전체 다 가져오기 
    Mutex::Locker l(dataq_lock); 
    dataq.swap(items); 
  }
  void batch_unpop_dataq(list<OpSequencer*> &items){
    Mutex::Locker l(dataq_lock);
    dataq.splice(dataq.begin(), items);
  }
  
  class DataWriteThread : public Thread {
    BuddyStore *bs;
  public:
    explicit DataWriteThread(BuddyStore *bs_) : bs(bs_) {}
    void *entry() override {
      bs->data_write_thread_entry();
      return 0;
    }
  } data_write_thread;


  bool stop_data_write;

  void data_write_thread_entry();


  ////////////////////////////////////////////////////
  // index writer  
  //
  ////////////////////////////////////////////////////
  
  class IndexWriteThread : public Thread {
    BuddyStore *bs;
  public:
    explicit IndexWriteThread(BuddyStore *bs_) : bs(bs_) {}
    void *entry() override {
      bs->index_write_thread_entry();
      return 0;
    }
  } index_write_thread;


  void index_write_thread_entry();
  int index_write_sync();

  Mutex index_write_lock;
  bool stop_index_write; // protected by index_write_lock 
  Cond index_write_cond; // protected by index_write_lock 

  double m_buddystore_index_sync_interval;
#if 0
  // 원래 q 는 해당 thread 에게 work 를 만들어서 던질때 필요한건데
  // index writer 의 경우에는.. 이미 있는 것들을 그냥 .. 쓰는 거니까. 필요없을듯. 
  Mutex index_writeq_lock;
  Cond index_writeq_cond; // 
  list<Op*> index_writeq;
  bool index_writeq_empty();
#endif
#if 0
  Op &peek_index_write();
  void pop_index_write();
//  void batch_pop_write(list<write_item> &items);
//  void batch_unpop_write(list<write_item> &items);

#endif

  ////////////////////////////////////////////////////
  //
  // omap store  
  //
  ////////////////////////////////////////////////////
  std::string omap_backend;
  std::string omap_dir;
    
  // ObjectMap for metadata
  boost::scoped_ptr<ObjectMap> object_kvmap;
  bool kvmap_exist;
  bool do_upgrade;


#endif // EUNJI 

  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_map_lock;    ///< rwlock to protect coll_map
  // EUNJI 
  //ceph::unordered_map<coll_t, BuddyLogDataFileObject*> coll_file_map;
  //RWLock coll_file_lock;

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
