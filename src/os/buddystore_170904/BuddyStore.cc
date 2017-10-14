 // -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "acconfig.h"

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "BuddyStore.h"
#include "include/compat.h"

#define dout_context cct
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddystore "
//#define dout_prefix *_dout << "buddystore(" << basedir << ") "




/*******************
 * constructor 
****************/

BuddyStore::BuddyStore(CephContext *cct, const string& basedir_, 
    osflagbits_t flags, const char *name ) : 
  BDJournalingObjectStore(cct, basedir_),
  internal_name(name),
  basedir(basedir_),
 // journalpath(basedir_ + "/buddy.jnl"),
  journalpath("/dev/xvdb1"),
  generic_flags(flags),
  logger(NULL),
  data_directio(cct->_conf->buddystore_data_directio), // default is true
  data_flush(cct->_conf->buddystore_data_flush),
  data_sync(cct->_conf->buddystore_data_sync),
  //data_directio(true),
  //data_write_thread(this),
  //data_writeq_lock("BuddyStore::data_writeq_lock"),
  data_write_stop(true),
  last_checkpointed_seq(0),
  next_osr_id(0),
  //journal_finish_lock("BuddyStore::journal_finish_lock"),
  m_journal_dio(cct->_conf->journal_dio),
  m_journal_aio(cct->_conf->journal_aio),
  m_journal_force_aio(cct->_conf->journal_force_aio),
  op_tp(cct, "BuddyStore::op_tp", "tp_bdstore_op", cct->_conf->buddystore_op_threads, "buddystore_op_threads"),
  op_wq(this, cct->_conf->filestore_op_thread_timeout,
      cct->_conf->buddystore_op_thread_suicide_timeout, &op_tp),
  data_op_tp(cct, "BuddyStore::data_op_tp", "tp_bdstore_dop", cct->_conf->buddystore_op_threads, "buddystore_op_threads"),
  data_op_wq(this, cct->_conf->filestore_op_thread_timeout,
      cct->_conf->buddystore_op_thread_suicide_timeout, &data_op_tp),
  coll_lock("BuddyStore::coll_lock"),
  coll_file_lock("BuddyStore::coll_file_lock"),
  ondisk_finisher(cct),
  apply_finisher(cct),
  ondisk_finisher_lock("BuddyStore::ondisk_fin_lock"),
  used_bytes(0) 
{

  // initialize logger
  PerfCountersBuilder plb(cct, internal_name, l_buddystore_first, l_buddystore_last);

  plb.add_u64(l_buddystore_journal_queue_ops, "journal_queue_ops", "Operations in journal queue");
  plb.add_u64(l_buddystore_journal_ops, "journal_ops", "Active journal entries to be applied");
  plb.add_u64(l_buddystore_journal_queue_bytes, "journal_queue_bytes", "Size of journal queue");
  plb.add_u64(l_buddystore_journal_bytes, "journal_bytes", "Active journal operation size to be applied");
  plb.add_time_avg(l_buddystore_journal_latency, "journal_latency", "Average journal queue completing latency");
  plb.add_u64_counter(l_buddystore_journal_wr, "journal_wr", "Journal write IOs");
  plb.add_u64_avg(l_buddystore_journal_wr_bytes, "journal_wr_bytes", "Journal data written");
  plb.add_u64_avg(l_buddystore_data_wr_bytes, "data_wr_bytes", "File data written");
  plb.add_time_avg(l_buddystore_data_wr_latency, "data_wr_latency", "Average data write completing latency");
  plb.add_time_avg(l_buddystore_journal_all_latency, "journal_all_latency", "Average all journal completing latency");
  plb.add_u64(l_buddystore_op_queue_max_ops, "op_queue_max_ops", "Max operations in writing to FS queue");
  plb.add_u64(l_buddystore_op_queue_ops, "op_queue_ops", "Operations in writing to FS queue");
  plb.add_u64_counter(l_buddystore_ops, "ops", "Operations written to store");
  plb.add_u64(l_buddystore_op_queue_max_bytes, "op_queue_max_bytes", "Max data in writing to FS queue");
  plb.add_u64(l_buddystore_op_queue_bytes, "op_queue_bytes", "Size of writing to FS queue");
  plb.add_u64_counter(l_buddystore_bytes, "bytes", "Data written to store");
  plb.add_time_avg(l_buddystore_apply_latency, "apply_latency", "Apply latency");
  plb.add_u64(l_buddystore_committing, "committing", "Is currently committing");

  plb.add_u64_counter(l_buddystore_commitcycle, "commitcycle", "Commit cycles");
  plb.add_time_avg(l_buddystore_commitcycle_interval, "commitcycle_interval", "Average interval between commits");
  plb.add_time_avg(l_buddystore_commitcycle_latency, "commitcycle_latency", "Average latency of commit");
  plb.add_u64_counter(l_buddystore_journal_full, "journal_full", "Journal writes while full");
  plb.add_time_avg(l_buddystore_queue_transaction_latency_avg, "queue_transaction_latency_avg", "Store operation queue latency");

  logger = plb.create_perf_counters();

  cct->get_perfcounters_collection()->add(logger);
  //cct->_conf->add_observer(this);


}
//  BuddyStore() {}/

BuddyStore::~BuddyStore() { }



// for comparing collections for lock ordering
bool operator>(const BuddyStore::CollectionRef& l,
	       const BuddyStore::CollectionRef& r)
{
  return (unsigned long)l.get() > (unsigned long)r.get();
}

void BuddyStore::new_journal()
{
  if (journalpath.length()) {
    dout(10) << "open_journal at " << journalpath << dendl;
    journal = new BuddyJournal(cct, fsid, &finisher, &sync_cond,
			      journalpath.c_str(),
			      data_flush, m_journal_dio, m_journal_aio,
			      m_journal_force_aio);
    if (journal)
      journal->logger = logger;
  }
  return;
}


int BuddyStore::mkjournal()
{
  dout(5) << __func__ << dendl;
  // read fsid
  fsid = get_fsid();

  new_journal();

  int ret = 0;
  if (journal) {
    ret = journal->check();
    if (ret < 0) {
      ret = journal->create();
      if (ret)
	derr << "mkjournal error creating journal on " << journalpath
		<< ": " << cpp_strerror(ret) << dendl;
      else
	dout(5) << "mkjournal created journal on " << journalpath << dendl;
    }
    delete journal;
    journal = 0;
  }
  return ret;
}


int BuddyStore::mount()
{
  dout(5) << __func__ << dendl;

  int r = _load();
  if (r < 0)
    return r;

  ondisk_finisher.start();
  apply_finisher.start();

#ifdef EUNJI
  uint64_t initial_op_seq = last_checkpointed_seq; 

  fsid = get_fsid();

  new_journal();

  // select journal mode?
  if (journal) {
    // backend filesystem 에 따라서 어떻게 할지 정하는건데 
    // 일단 buddystore 는 write-ahead logging 을 하는게 맞음. 
    // map 을 checkpoint 하기 전까지는 지우면 안되니까. 
    journal->set_wait_on_full(true);
    dout(5) << "journal write_ahead mode " << dendl;
  }

  //if (!(generic_flags & SKIP_JOURNAL_REPLAY))
  journal_replay(initial_op_seq); // mount 할 때 이거 해줘야 함. 

  // start journal thread 
  journal_start();

  op_tp.start();
  data_op_tp.start();

  // data_write_thread
  //start_data_write_thread();

#endif


#ifdef ALICIA
  bdfs.mount();
#endif
  return 0;
}

int BuddyStore::umount()
{

  dout(5) << __func__ << dendl;
#ifdef EUNJI
  // sync 먼저 

  // commit = checkpoint 
  // 이걸 해야 journal 공간이 회수가 됨. 
  if(apply_manager.commit_start()) {
    // committing_seq setting 
    uint64_t cp = apply_manager.get_committing_seq();
    last_checkpointed_seq = cp;
  
    apply_manager.commit_started();
    apply_manager.commit_finish();
  
  }

  journal_stop();
  //if (!(generic_flags & SKIP_JOURNAL_REPLAY))
    journal_write_close();

  // stop_data_write_thread();
#endif

  op_tp.stop();
  data_op_tp.stop();

  ondisk_finisher.wait_for_empty();
  ondisk_finisher.stop();

  apply_finisher.wait_for_empty();
  apply_finisher.stop();


#ifdef ALICIA
  bdfs.umount();
#endif
  return _save();
}

int BuddyStore::_save()
{
  dout(10) << __func__ << dendl;

  //dump_all();
  set<coll_t> collections;
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    dout(20) << __func__ << " coll " << p->first << " " << p->second << dendl;
    collections.insert(p->first);
    bufferlist bl;
    assert(p->second);
    /// collection encode 
    p->second->encode(bl);

    // EUNJI: coll_file_map encode 
    // 만약 coll file 이 있다면 그것도 같이 encode 하자. 
    {
      RWLock::WLocker l(coll_file_lock);
      int exist = 0;
      ceph::unordered_map<coll_t, BuddyHashIndexFile*>::iterator fp = coll_file_map.find(p->first);
      if(fp != coll_file_map.end()){
	exist = 1;
	::encode(exist, bl);
	// hash map file encode 
	fp->second->encode(bl);
	fp->second->close_file();
	dout(10) << __func__ << " coll_file_map cid " << p->first << " bl " << bl.length() << dendl;
	exist = 1;
      } else {
	::encode(exist, bl);
      }	
    }
    string fn = path + "/" + stringify(p->first);
    int r = bl.write_file(fn.c_str());
    if (r < 0)
      return r;

#if 0
#ifdef EUNJI
    //p->second->data_file.close_file();
    //
    {
      RWLock::WLocker l(coll_file_lock);
      ceph::unordered_map<coll_t, BuddyHashIndexFile*>::iterator fp = coll_file_map.find(p->first);
      if(fp != coll_file_map.end()){
	// hash map file encode 
	fp->second->encode(hbl);
	fp->second->close_file();
	dout(10) << __func__ << " coll_file_map cid " << p->first << " hbl " << hbl.length() << dendl;
      }
    }
#endif
#endif
  }

  string fn = path + "/collections";
  bufferlist bl;
  ::encode(collections, bl);
  int r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;


#if 0
#ifdef EUNJI
  // write last_checkpointed_seq
  fn = path + "/coll_file_map";

  r = hbl.write_file(fn.c_str());
  dout(10) << __func__ << " coll_file_map length " << hbl.length() << dendl;

  if (r < 0){
    dout(10) << "Failed to write coll_file_map" << dendl;
    return r;
  }
#endif
#endif

#ifdef EUNJI
  // write last_checkpointed_seq
  dout(10) << __func__ << " last_checkpointed_seq " << last_checkpointed_seq << dendl;
  fn = path + "/last_checkpointed_seq";
  bufferlist cbl;
  ::encode(last_checkpointed_seq, cbl);
  r = cbl.write_file(fn.c_str());
  if (r < 0){
    dout(10) << "Failed to write last_checkpointed seq " << dendl;
    return r;
  }
#endif

  return 0;
}

void BuddyStore::dump_all()
{
  Formatter *f = Formatter::create("json-pretty");
  f->open_object_section("store");
  dump(f);
  f->close_section();
  dout(5) << "dump:";
  f->flush(*_dout);
  *_dout << dendl;
  delete f;
}

void BuddyStore::dump(Formatter *f)
{
  f->open_array_section("collections");
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    f->open_object_section("collection");
    f->dump_string("name", stringify(p->first));

    f->open_array_section("xattrs");
    for (map<string,bufferptr>::iterator q = p->second->xattr.begin();
	 q != p->second->xattr.end();
	 ++q) {
      f->open_object_section("xattr");
      f->dump_string("name", q->first);
      f->dump_int("length", q->second.length());
      f->close_section();
    }
    f->close_section();

    f->open_array_section("objects");
    for (map<ghobject_t,ObjectRef>::iterator q = p->second->object_map.begin();
	 q != p->second->object_map.end();
	 ++q) {
      f->open_object_section("object");
      f->dump_string("name", stringify(q->first));
      if (q->second)
	q->second->dump(f);
      f->close_section();
    }
    f->close_section();

    f->close_section();
  }
  f->close_section();
}

int BuddyStore::_load()
{
  dout(10) << __func__ << dendl;
  bufferlist bl;
  string fn = path + "/collections";
  string err;
  int r = bl.read_file(fn.c_str(), &err);
  if (r < 0)
    return r;

#if 0
#ifdef EUNJI
  // write last_checkpointed_seq
  dout(10) << __func__ << " coll_file_map " << dendl;
  fn = path + "/coll_file_map";

  bufferlist hbl;
  bufferlist::iterator hp;

  bool coll_file_map_exist = false;
  r = hbl.read_file(fn.c_str(), &err);
  if (r < 0){
    dout(10) << "Failed to read coll_file_map" << dendl;
  } else {
    dout(10) << __func__ << " coll_file_map length " << hbl.length << dendl;
    coll_file_map_exist = true;
    hp = hbl.begin();
  }

  // 여기서 coll_map 파일 복구해야하는구나.. 
#endif
#endif

  set<coll_t> collections;
  bufferlist::iterator p = bl.begin();
  ::decode(collections, p);

  for (set<coll_t>::iterator q = collections.begin();
       q != collections.end();
       ++q) {
    string fn = path + "/" + stringify(*q);
    bufferlist cbl;
    int r = cbl.read_file(fn.c_str(), &err);
    if (r < 0)
      return r;
    CollectionRef c(new Collection(cct, basedir, *q));
    bufferlist::iterator p = cbl.begin();
    // collection decode 
    c->decode(p);

#ifdef EUNJI
    int exist = -1;
    ::decode(exist, p);

    if (exist) {
      BuddyHashIndexFile* datafile = new BuddyHashIndexFile(cct, (*q), basedir + "/" + (*q).to_str() + ".data", data_directio);
      datafile->decode(p); 
      coll_file_map.insert(make_pair((*q), datafile));
    }
#endif
    coll_map[*q] = c;
    used_bytes += c->used_bytes();
  }


#ifdef EUNJI
  bufferlist cbl;
  fn = path + "/last_checkpointed_seq";
  r = cbl.read_file(fn.c_str(), &err);
  if (r < 0) {
    ::encode(last_checkpointed_seq, cbl);
    r = cbl.write_file(fn.c_str());
    if (r < 0) {
      dout(10) << "Failed to write " << fn << dendl;
    }
  } else {
    p = cbl.begin();  
    ::decode(last_checkpointed_seq, p);
  }
  dout(10) << "read last checkpoint: " << last_checkpointed_seq << dendl;
#endif

  //dump_all();

  return 0;
}

void BuddyStore::set_fsid(uuid_d u)
{
  int r = write_meta("fs_fsid", stringify(u));
  assert(r >= 0);
}

uuid_d BuddyStore::get_fsid()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  assert(r >= 0);
  uuid_d uuid;
  bool b = uuid.parse(fsid_str.c_str());
  assert(b);
  return uuid;
}

int BuddyStore::mkfs()
{
  string fsid_str;
  int r = read_meta("fs_fsid", &fsid_str);
  if (r == -ENOENT) {
    uuid_d fsid;
    fsid.generate_random();
    fsid_str = stringify(fsid);
    r = write_meta("fs_fsid", fsid_str);
    if (r < 0)
      return r;
    dout(1) << __func__ << " new fsid " << fsid_str << dendl;
  } else if (r < 0) {
    return r;
  } else {  
    dout(1) << __func__ << " had fsid " << fsid_str << dendl;
  }

  string fn = path + "/collections";
  derr << path << dendl;
  bufferlist bl;
  set<coll_t> collections;
  ::encode(collections, bl);
  r = bl.write_file(fn.c_str());
  if (r < 0)
    return r;

  // journal? 
  r = mkjournal(); 
  dout(10) << "return of mkjournal() " << r << dendl;


  r = write_meta("type", "buddystore");
  if (r < 0)
    return r;

#ifdef ALICIA
  bdfs.buddy_mkfs();
#endif
  return 0;
}

int BuddyStore::statfs(struct store_statfs_t *st)
{
   dout(10) << __func__ << dendl;
  st->reset();
  st->total = cct->_conf->buddystore_device_bytes;
  st->available = MAX(int64_t(st->total) - int64_t(used_bytes), 0ll);
  dout(10) << __func__ << ": used_bytes: " << used_bytes
	   << "/" << cct->_conf->buddystore_device_bytes << dendl;
  return 0;
}

objectstore_perf_stat_t BuddyStore::get_cur_stats()
{
  // fixme
  return objectstore_perf_stat_t();
}

BuddyStore::CollectionRef BuddyStore::get_collection(const coll_t& cid)
{
  RWLock::RLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}


// ---------------
// read operations

bool BuddyStore::exists(const coll_t& cid, const ghobject_t& oid)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return false;
  return exists(c, oid);
}

bool BuddyStore::exists(CollectionHandle &c_, const ghobject_t& oid)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->get_cid() << " " << oid << dendl;
  if (!c->exists)
    return false;

  // Perform equivalent of c->get_object_(oid) != NULL. In C++11 the
  // shared_ptr needs to be compared to nullptr.
  return (bool)c->get_object(oid);
}

int BuddyStore::stat(
    const coll_t& cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return stat(c, oid, st, allow_eio);
}

int BuddyStore::stat(
  CollectionHandle &c_,
  const ghobject_t& oid,
  struct stat *st,
  bool allow_eio)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  st->st_size = o->get_size();
  st->st_blksize = 4096;
  st->st_blocks = (st->st_size + st->st_blksize - 1) / st->st_blksize;
  st->st_nlink = 1;
  return 0;
}

int BuddyStore::set_collection_opts(
  const coll_t& cid,
  const pool_opts_t& opts)
{
  return -EOPNOTSUPP;
}

int BuddyStore::read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags,
    bool allow_eio)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return read(c, oid, offset, len, bl, op_flags, allow_eio);
}

int BuddyStore::read(
  CollectionHandle &c_,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  if (!c->exists)
    return -ENOENT;
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  if (offset >= o->get_size())
    return 0;
  size_t l = len;
  if (l == 0 && offset == 0)  // note: len == 0 means read the entire object
    l = o->get_size();
  else if (offset + l > o->get_size())
    l = o->get_size() - offset;
  bl.clear();


  /// 여기서 받아오는데 
  // 예의상 bufferlist 의 read 함수 한번 부르고 hash file read 로 넘어감. 
  // 나중에 뭔가 caching 할 떄를 대비해서. 
  int ret = o->read(offset, l, bl);

  if(ret < static_cast<int>(len)) {

    BuddyHashIndexFile* data_file;

    // coll file 찾아서 넘겨야 함.     
    RWLock::RLocker l(coll_file_lock);

    ceph::unordered_map<coll_t, BuddyHashIndexFile*>::iterator p = coll_file_map.find(c->cid);

    if(p != coll_file_map.end()){
      data_file = p->second;
    } else {
      dout(5) << __func__ << " Failed to find collection data file cid " << 
	c->cid << dendl;
      return -1;
    }
      
    // read ..    
    vector<buddy_iov_t> iov;
    int ret = data_file->get_space_info(oid, offset, len, iov);
    if (ret < 0)
      return -ENOENT;

    for(vector<buddy_iov_t>::iterator iovp = iov.begin(); 
	iovp != iov.end(); iovp++) {

      ret += data_file->read_fd(bl, (*iovp).foff, (*iovp).bytes);

    }
    // return o->read(offset, l, bl);
  }

  dout(10) << __func__ << " read bytes " << ret << dendl;

  return ret;
}

int BuddyStore::fiemap(const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, bufferlist& bl)
{
  map<uint64_t, uint64_t> destmap;
  int r = fiemap(cid, oid, offset, len, destmap);
  if (r >= 0)
    ::encode(destmap, bl);
  return r;
}

int BuddyStore::fiemap(const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, map<uint64_t, uint64_t>& destmap)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  size_t l = len;
  if (offset + l > o->get_size())
    l = o->get_size() - offset;
  if (offset >= o->get_size())
    goto out;
  destmap[offset] = l;
 out:
  return 0;
}

int BuddyStore::getattr(const coll_t& cid, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return getattr(c, oid, name, value);
}

int BuddyStore::getattr(CollectionHandle &c_, const ghobject_t& oid,
		      const char *name, bufferptr& value)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << " " << name << dendl;
  if (!c->exists)
    return -ENOENT;
  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  string k(name);
  std::lock_guard<std::mutex> lock(o->xattr_mutex);
  if (!o->xattr.count(k)) {
    return -ENODATA;
  }
  value = o->xattr[k];
  return 0;
}

int BuddyStore::getattrs(const coll_t& cid, const ghobject_t& oid,
		       map<string,bufferptr>& aset)
{
  CollectionHandle c = get_collection(cid);
  if (!c)
    return -ENOENT;
  return getattrs(c, oid, aset);
}

int BuddyStore::getattrs(CollectionHandle &c_, const ghobject_t& oid,
		       map<string,bufferptr>& aset)
{
  Collection *c = static_cast<Collection*>(c_.get());
  dout(10) << __func__ << " " << c->cid << " " << oid << dendl;
  if (!c->exists)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->xattr_mutex);
  aset = o->xattr;
  return 0;
}

int BuddyStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << __func__ << dendl;
  RWLock::RLocker l(coll_lock);
  for (ceph::unordered_map<coll_t,CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end();
       ++p) {
    ls.push_back(p->first);
  }
  return 0;
}

bool BuddyStore::collection_exists(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::RLocker l(coll_lock);
  return coll_map.count(cid);
}

int BuddyStore::collection_empty(const coll_t& cid, bool *empty)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  *empty = c->object_map.empty();
  return 0;
}

int BuddyStore::collection_bits(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);
  return c->bits;
}

int BuddyStore::collection_list(const coll_t& cid,
			      const ghobject_t& start,
			      const ghobject_t& end,
			      int max,
			      vector<ghobject_t> *ls, ghobject_t *next)
{
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  RWLock::RLocker l(c->lock);

  dout(20) << __func__ << " cid " << cid << " start " << start
	   << " end " << end << dendl;
  map<ghobject_t,ObjectRef>::iterator p = c->object_map.lower_bound(start);
  while (p != c->object_map.end() &&
	 ls->size() < (unsigned)max &&
	 p->first < end) {
    ls->push_back(p->first);
    ++p;
  }
  if (next != NULL) {
    if (p == c->object_map.end())
      *next = ghobject_t::get_max();
    else
      *next = p->first;
  }
  dout(20) << __func__ << " cid " << cid << " got " << ls->size() << dendl;
  return 0;
}

int BuddyStore::omap_get(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  *header = o->omap_header;
  *out = o->omap;
  return 0;
}

int BuddyStore::omap_get_header(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio ///< [in] don't assert on eio
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  *header = o->omap_header;
  return 0;
}

int BuddyStore::omap_get_keys(
    const coll_t& cid,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  for (map<string,bufferlist>::iterator p = o->omap.begin();
       p != o->omap.end();
       ++p)
    keys->insert(p->first);
  return 0;
}

int BuddyStore::omap_get_values(
    const coll_t& cid,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*q);
  }
  return 0;
}

int BuddyStore::omap_check_keys(
    const coll_t& cid,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
    )
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  for (set<string>::const_iterator p = keys.begin();
       p != keys.end();
       ++p) {
    map<string,bufferlist>::iterator q = o->omap.find(*p);
    if (q != o->omap.end())
      out->insert(*p);
  }
  return 0;
}

class BuddyStore::OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  CollectionRef c;
  ObjectRef o;
  map<string,bufferlist>::iterator it;
public:
  OmapIteratorImpl(CollectionRef c, ObjectRef o)
    : c(c), o(o), it(o->omap.begin()) {}

  int seek_to_first() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    it = o->omap.begin();
    return 0;
  }
  int upper_bound(const string &after) override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    it = o->omap.upper_bound(after);
    return 0;
  }
  int lower_bound(const string &to) override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    it = o->omap.lower_bound(to);
    return 0;
  }
  bool valid() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    return it != o->omap.end();
  }
  int next(bool validate=true) override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    ++it;
    return 0;
  }
  string key() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    return it->first;
  }
  bufferlist value() override {
    std::lock_guard<std::mutex>(o->omap_mutex);
    return it->second;
  }
  int status() override {
    return 0;
  }
};

ObjectMap::ObjectMapIterator BuddyStore::get_omap_iterator(const coll_t& cid,
							 const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return ObjectMap::ObjectMapIterator();

  ObjectRef o = c->get_object(oid);
  if (!o)
    return ObjectMap::ObjectMapIterator();
  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o));
}


// ---------------
// write operations


ostream& operator<<(ostream& out, const BuddyStore::OpSequencer& s)
{
  return out << *s.parent;
}


struct C_JournalCompletion : public Context {
  BuddyStore *fs;
  BuddyStore::OpSequencer *osr;
  BuddyStore::Op *o;
  Context *ondisk;

  C_JournalCompletion(BuddyStore *f, BuddyStore::OpSequencer *os, BuddyStore::Op *o, Context *ondisk):
    fs(f), osr(os), o(o), ondisk(ondisk) { }
  void finish(int r) override {
    fs->_finish_journal(osr, o, ondisk);
  }
};


int BuddyStore::queue_transactions(Sequencer *posr,
				 vector<Transaction>& tls,
				 TrackedOpRef osd_op,
				 ThreadPool::TPHandle *handle)
{

  dout(10) << __func__ << "transactions = " << tls.size() << dendl;


  // Context .. 
  Context *onreadable = NULL, *onreadable_sync = NULL, *ondisk = NULL;
  ObjectStore::Transaction::collect_contexts(tls, &onreadable, &ondisk,
					     &onreadable_sync);

  dout(10) << osd_op << dendl;


  utime_t start = ceph_clock_now();

  // set up the sequencer
  OpSequencer *osr;
  assert(posr);
  if (posr->p) {
    // 윗단계에서 같은 osr 를 셋업해서 넘겨줄수도 있구먼.. 
    osr = static_cast<OpSequencer *>(posr->p.get());
    dout(5) << "queue_transactions existing " << osr << " " << *osr << dendl;
  } else {
    osr = new OpSequencer(cct, next_osr_id.inc());
    osr->set_cct(cct);
    osr->parent = posr;
    posr->p = osr;
    dout(5) << "queue_transactions new " << osr << " " << *osr << dendl;
  }

  // used to include osr information in tracepoints during transaction apply
  for (vector<Transaction>::iterator i = tls.begin(); i != tls.end(); ++i) {
    (*i).set_osr(osr);
  }

  // build op 
  Op *o = build_op(tls, onreadable, onreadable_sync, ondisk, osd_op);


  // genearte iovector 
  int r;
 
  if (data_flush) {
    r = generate_iov(o->tls, o->tls_iov);
    if (r < 0) {
      dout(10) << "Failed to generate iov" << dendl;
    }
  }

  /*************************
   * start io 
   * 1. journal 
   * 1. vector io (in parallel) 
   * -------------------
   * 2. do_transaction 
   *************************/

  dout(10) << "journal prepare entry " << dendl;
  // send journal 
 
  if(journal){
    

    bufferlist tbl; // prepare_entry 를 부르면 여기에 data 가 담겨서 옴. 

    int orig_len = journal->prepare_entry(o->tls, &tbl);
    
    journal->reserve_throttle_and_backoff(tbl.length());


    // start journaling 
    uint64_t op_num = submit_manager.op_submit_start();
    o->op = op_num;

    osr->queue_journal(o);

    if (data_flush)
      osr->set_jcount(op_num, 2);
    else
      osr->set_jcount(op_num, 1);

    _op_journal_transactions(tbl, orig_len, o->op, new C_JournalCompletion(this, osr, o, ondisk), osd_op);
    submit_manager.op_submit_finish(op_num);

    utime_t end = ceph_clock_now();
    // 큐잉 하는 데에만 걸리는 시간 측정. 
    logger->tinc(l_buddystore_queue_transaction_latency_avg, end - start);

    // start data io 
    if (data_flush)
      queue_data_op(osr, o);

    return 0;
  } // journal 


  // !journal

  r = _do_transactions(o->tls, o->op, handle);

  o->tls.clear();
  // unregister_inflight_op .. 
  delete o;

  if (onreadable_sync)
    onreadable_sync->complete(0);
  if (onreadable)
    apply_finisher.queue(onreadable);

  if (!journal && ondisk){
    Mutex::Locker locker(ondisk_finisher_lock);
    ondisk_finisher.queue(ondisk);
  }


  return 0;
}


// journal writer 랑 op_wq 쓰레드 모두 logging 끝나며 이거 불러야 함
void BuddyStore::_finish_journal (OpSequencer *osr, Op *o, Context *ondisk)
{

  //dout(5) << "_finish_journal " << o << " seq " << o->op << " " << *osr << " " << o->tls << dendl;

  // 여기서 그냥가지 말고.. ondisk 를 어딘가에 넣어주도록 하면 되겠음. osr에. 
  // count 체크하는거랑 queue 에 매다는 거하고 동시에 해야함. 
  int r = osr->dec_jcount(o->op);
  
  dout(10) << __func__ << " seq " << o->op << " jcount = " << r << dendl;  

  if (r > 0){
    list<Context*> to_queue;
    osr->dequeue_journal(&to_queue);
    return;
  }

  // jcount = 0 
  // verification 
  Op* dop = osr->peek_queue_data();
  if(dop)
    assert(o->op <= dop->op);

  queue_op(osr, o);

  // 저널 큐에서 빼기 전에 넣어야 함.  queue_op 에서 osr 의 q 로도 넣어주고 
  // op_wq.queue 에도 넣어줌. op_wq 에 들어간 애들은 _do_op 랑 _finish_op 를 
  // 번갈아가면서 함.  
  // osr 의 q 에서 빼는건 _finish_op 에서 해줌. 
  // 저널링은 순서대로 끝나서 _finish_journal 에 들어와서 차례대로 불리우고 있으니까. 
  // 여기서 따로 lock 잡지 않아도 아마 journal 의 finisher 가 lock 
  // 잡고 이거 차례대로 넣었음. 그리고 finisher 한놈이니까 lock 필요없겟지.. 
  // this should queue in order because the journal does it's completions in order.
//  queue_op(osr, o);

  list<Context*> to_queue;
  osr->dequeue_journal(&to_queue);

  // do ondisk completions async, to prevent any onreadable_sync completions
  // getting blocked behind an ondisk completion.

  assert(o->ondisk == ondisk);

  if (ondisk) {
    Mutex::Locker locker(ondisk_finisher_lock);
    dout(20) << __func__ << "finisher_queue" << dendl;
    ondisk_finisher.queue(ondisk);
  //  dout(10) << " queueing ondisk " << ondisk << dendl;
    //ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(ondisk);
  }
  if (!to_queue.empty()) {
    Mutex::Locker locker(ondisk_finisher_lock);
    dout(20) << "to_queue is not empty" << dendl;
    ondisk_finisher.queue(to_queue);
    //ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(to_queue);
  }

  /// time 
  utime_t lat = ceph_clock_now();
  lat -= o->start;

  if (logger) {
    logger->tinc(l_buddystore_journal_all_latency, lat);
  }

  dout(5) << __func__ << " journal_all complete lat " << lat << dendl; 
}


#ifdef EUNJI
/***************
 * generate_iov_data_bl
 * iov 랑 bl 받아서 쪼개는 것임. 
 * 그런데 buddy_iov_t 의 ooff 가 원래는 주어진 bl 에서의 offset 을 말하는데 
 * 나중에 0으로 세팅 안해야 원래 write 되었을때 어디까지 쓰여졌는지..
 * tracking 할 수 있음. 
 * 예를 들어, 0 - 9 까지의 10개의 데이터가 0 - 2, 3 - 9 까지 
 * 따로따로 맵핑되어 쓰여졌다면 buddy_iov_t 의 ooff 가 첫번째는 0, 두번째는 3으로 써야할듯. 
 **************/
/********************
 * generate_iov()
 *  This function generate iovec and log bufferlist. 
 *  trim_map 을 Transaction 이 생성해주면 parsing 하는 단계 없을수도 있음. 
 *  그런데 parse_transaction 에서 context 만들어서- 
 *  나중에 처리할 수 있도록 하면 좋겠음. 
 *******************/
int BuddyStore::generate_iov(vector<Transaction>& tls, vector<buddy_iov_t>& tls_iov)
{
  dout(10) << __func__  << " transactions = " << tls.size() << dendl;

  // for debug 
  if(!data_flush){
    dout(5) << __func__ << " No data flush" << dendl;
    return 0;
  }

  // go through transaction. 
  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {

    Transaction tr = *p; 
    Transaction::iterator i = tr.begin();

    for(vector<Transaction::Op>::iterator op_p = tr.punch_hole_ops.begin();
	op_p != tr.punch_hole_ops.end(); ++op_p) {

      uint32_t punch_hole_off = op_p->punch_hole_off;
      uint32_t header_len = sizeof(__u32);

      dout(10) << "punch_hole exist " << " punch_hole_off " << punch_hole_off << " header_len " << header_len
	<< " op_len " << op_p->len << "op_off" << op_p->off << " data_bl length " << p->data_bl.length() << dendl;
      
      // 그런데 여기서 substr_of 를 해버리면- 
      // 실제 _do_transaction 할때 문제가 생길텐데
      // 근데 보니까 const 네. 그럼 안건드린다는 얘긴데. 
      //bufferlist write_data_bl;
      //write_data_bl.substr_of(p->data_bl, punch_hole_off + header_len, op_p->len);

      // alloc space of hash_index_file. 
      // split data into io vector.
      vector<buddy_iov_t> op_iov; 


      // op_p->off 는 해당 오브젝트에서 어느 위치인지를 나타내는듯. 
      uint64_t off = op_p->off;
      uint64_t len = op_p->len;

      coll_t cid = i.get_cid(op_p->cid);
      ghobject_t oid = i.get_oid(op_p->oid);

      // get collection data file 
      BuddyHashIndexFile* data_file; 

      // collection 이랑 file write 를 구분한 이유가 있었지. 
      // 이렇게 안하면 collection 생성을 반드시 해야해서.. 
      // transaction 처리하기 전에 보여져 버림.. 
      {
	RWLock::WLocker l(coll_file_lock);

	ceph::unordered_map<coll_t, BuddyHashIndexFile*>::iterator p = coll_file_map.find(cid);

	// not found 
	if (p == coll_file_map.end()) {
	  data_file = new BuddyHashIndexFile(cct, cid, basedir + "/" + cid.to_str() + ".data", data_directio);
	  coll_file_map.insert(std::make_pair(cid, data_file));
	} else {
	  data_file = p->second;
	}
      }
	
      // allocate space 
      int ret = data_file->alloc_space(BD_DATA_T, oid, off, len, op_iov);
      if (ret < 0){
	dout(10) << "[EUNJI] collision with off " << off << " len " << len << dendl;
	data_file->dump_hash_index_map();
	return -ENOENT;
      }

      // 여기에서 op_iov 에 vector 정보가 있을 것임. 
      generate_iov_data_bl(op_iov, p->data_bl, punch_hole_off + header_len);
      //generate_iov_data_bl(op_iov, write_data_bl);

      // add to iov. 
      // 원래 여기서 transaction 의 iov_t 도 만들어서 넣어줘야 함. . 
      tls_iov.insert(tls_iov.end(), op_iov.begin(), op_iov.end());

      // generate iov_t with buddy_iov_t 
      vector<Transaction::iov_t> tmp_iov;

      for(vector<buddy_iov_t>::iterator iovp = op_iov.begin();
	 iovp != op_iov.end(); ++iovp) {
	tmp_iov.push_back(Transaction::iov_t((*iovp).fname, (*iovp).foff, (*iovp).bytes));
      }
      tr.punch_hole_map.insert(make_pair(punch_hole_off, tmp_iov)); 

      dout(10) << "tls_iov count = " << tls_iov.size() << dendl;
    }
  } // end of tls loop

  dout(10) << __func__  << " iov = " << tls_iov.size() << dendl;
  return 0;
}
#endif
void BuddyStore::generate_iov_data_bl(vector<buddy_iov_t>& iov, bufferlist& bl, uint32_t start_off)
{
  dout(10) << __func__ << " iov count = " << iov.size() << dendl; 
 
 // iov 에서 ooff 는  
  for(vector<buddy_iov_t>::iterator ip = iov.begin();
      ip != iov.end(); ++ip){


    dout(10) << __func__ << " start_off " << start_off << " ooff " << (*ip).ooff << " len " << (*ip).bytes << " bl.length() " << bl.length() << dendl;
   
    bufferlist newdata;
    newdata.substr_of(bl, start_off + (*ip).ooff, (*ip).bytes);
    //newdata.substr_of(bl, (*ip).ooff, (*ip).ooff + (*ip).bytes);
    //(*ip).ooff = 0; // after bl split. 이건 원래 값 유지시킬 것.  

    (*ip).data_bl.claim(newdata);
    assert((*ip).bytes == (*ip).data_bl.length());
    dout(10) << iov << dendl;
  }

}


void BuddyStore::_do_transaction(Transaction& t, uint64_t op_seq, 
    int trans_num, ThreadPool::TPHandle *handle)
{

  dout(10) << __func__ << dendl;

  Transaction::iterator i = t.begin();
  int pos = 0;

  while (i.have_op()) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _touch(cid, oid);
      }
      break;

    case Transaction::OP_WRITE:
      {
	//EUNJI 
	dout(10) << " punch_hole_ops " << t.punch_hole_ops.size() << dendl;
	assert(t.punch_hole_ops.size() > 0);

        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl); 
	r = _write(cid, oid, off, len, bl, fadvise_flags); 
      }
      break;

    case Transaction::OP_ZERO:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(cid, oid, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        uint64_t off = op->off;
	r = _truncate(cid, oid, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _remove(cid, oid);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(cid, oid, to_set);

      }
      break;

    case Transaction::OP_SETATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(cid, oid, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string name = i.decode_string();
	r = _rmattr(cid, oid, name.c_str());
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _rmattrs(cid, oid);
      }
      break;

    case Transaction::OP_CLONE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
	r = _clone(cid, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _clone_range(cid, oid, noid, off, len, off);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(cid, oid, noid, srcoff, len, dstoff);

      }
      break;

    case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _create_collection(cid, op->split_bits);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint_type;
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          r = _collection_hint_expected_num_objs(cid, pg_num, num_objs);
        } else {
          // Ignore the hint
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = _destroy_collection(cid);
#ifdef ALICIA
	r = bdfs.buddy_destroy_collection(cid, *bt);
#endif
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
        coll_t ocid = i.get_cid(op->cid);
        coll_t ncid = i.get_cid(op->dest_cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _collection_add(ncid, ocid, oid);
#ifdef ALICIA
	r = bdfs.buddy_collection_add(ncid, ocid, oid, *bt);
#endif
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _remove(cid, oid);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      assert(0 == "deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        coll_t newcid = i.get_cid(op->dest_cid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	r = _collection_move_rename(oldcid, oldoid, newcid, newoid);
#ifdef ALICIA
	r = bdfs.buddy_collection_move_rename(oldcid, oldoid, newcid, newoid, *bt);
#endif
	if (r == -ENOENT)
	  r = 0;
      }
      break;

    case Transaction::OP_TRY_RENAME:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	r = _collection_move_rename(cid, oldoid, cid, newoid);
#ifdef ALICIA
	r = bdfs.buddy_collection_move_rename(cid, oldoid, cid, newoid, *bt);
#endif
	if (r == -ENOENT)
	  r = 0;
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	assert(0 == "not implemented");
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	assert(0 == "not implemented");
      }
      break;

    case Transaction::OP_COLL_RENAME:
      {
	assert(0 == "not implemented");
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	r = _omap_clear(cid, oid);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
	r = _omap_setkeys(cid, oid, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	r = _omap_rmkeys(cid, oid, keys_bl);
#ifdef ALICIA
	r = bdfs.buddy_omap_rmkeys(cid, oid, keys_bl, *bt);
#endif
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
	r = _omap_rmkeyrange(cid, oid, first, last);
#ifdef ALICIA
	r = bdfs.buddy_omap_rmkeyrange(cid, oid, first, last, *bt);
#endif
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
        bufferlist bl;
        i.decode_bl(bl);
	r = _omap_setheader(cid, oid, bl);
#ifdef ALICIA
	r = bdfs.buddy_omap_setheader(cid, oid, bl, *bt);
#endif
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      assert(0 == "deprecated");
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        coll_t dest = i.get_cid(op->dest_cid);
	r = _split_collection(cid, bits, rem, dest);
#ifdef ALICIA
	r = bdfs.buddy_split_collection(cid, bits, rem, dest, *bt);
#endif
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        r = 0;
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      ceph_abort();
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC from BuddyStore, misconfigured cluster or insufficient memory";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	  dump_all();
	}

	derr    << " error " << cpp_strerror(r) << " not handled on operation " << op->op
		<< " (op " << pos << ", counting from 0)" << dendl;
	dout(5) << msg << dendl;
	dout(5) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	assert(0 == "unexpected error");
      }
    }

    ++pos;
  }


#ifdef ALICIA
  int r;
  r = bdfs.buddy_do_commit(*bt);
  if (r > 0) // things to do after commit 
    r = bdfs.buddy_end_commit(*bt);
  assert(r == 0);
#endif

}


int BuddyStore::_touch(const coll_t& cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  c->get_or_create_object(oid);
  return 0;
}


int BuddyStore::_write(const coll_t& cid, const ghobject_t& oid,
		     uint64_t offset, size_t len, const bufferlist& bl,
		     uint32_t fadvise_flags)
{

  dout(10) << __func__ << " " << cid << " " << oid << " "
	   << offset << "~" << len << dendl;
  assert(len == bl.length());

  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_or_create_object(oid);
  if (len > 0) {
    const ssize_t old_size = o->get_size();
    o->write(offset, bl);
    used_bytes += (o->get_size() - old_size);
  }
  return 0;
}


int BuddyStore::_zero(const coll_t& cid, const ghobject_t& oid,
		    uint64_t offset, size_t len)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << offset << "~"
	   << len << dendl;
  bufferlist bl;
  bl.append_zero(len);
  return _write(cid, oid, offset, len, bl);
}

int BuddyStore::_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << size << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  const ssize_t old_size = o->get_size();
  int r = o->truncate(size);
  used_bytes += (o->get_size() - old_size);
  return r;
}

int BuddyStore::_remove(const coll_t& cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  {
  RWLock::WLocker l(c->lock);

  auto i = c->object_hash.find(oid);
  if (i == c->object_hash.end())
    return -ENOENT;
  used_bytes -= i->second->get_size();
  c->object_hash.erase(i);
  c->object_map.erase(oid);
  }

  {
    // release space 불러야 할듯. 
    BuddyHashIndexFile* data_file;
    
    // coll file 찾아서 넘겨야 함.     
    RWLock::RLocker l(coll_file_lock);

    // collection 중에 file 에 data 를 실제 쓴 object  가 있을 때만 
    // coll_file_map 에 추가가 됨. 
    // 그게 아닌 경우에는 coll_map 에만 있음. 
    // 그래서 못찾더라도 에러는 아님. 
    ceph::unordered_map<coll_t, BuddyHashIndexFile*>::iterator p = coll_file_map.find(c->cid);

    if(p != coll_file_map.end()){
      data_file = p->second;
      data_file->release_space(oid);
    } else {
      dout(5) << __func__ << " Collection has no data file cid " << 
	c->cid << dendl;
    }
  }

  return 0;
}

int BuddyStore::_setattrs(const coll_t& cid, const ghobject_t& oid,
			map<string,bufferptr>& aset)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->xattr_mutex);
  for (map<string,bufferptr>::const_iterator p = aset.begin(); p != aset.end(); ++p)
    o->xattr[p->first] = p->second;
  return 0;
}

int BuddyStore::_rmattr(const coll_t& cid, const ghobject_t& oid, const char *name)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << name << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->xattr_mutex);
  auto i = o->xattr.find(name);
  if (i == o->xattr.end())
    return -ENODATA;
  o->xattr.erase(i);
  return 0;
}

int BuddyStore::_rmattrs(const coll_t& cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->xattr_mutex);
  o->xattr.clear();
  return 0;
}

int BuddyStore::_clone(const coll_t& cid, const ghobject_t& oldoid,
		     const ghobject_t& newoid)
{
  dout(10) << __func__ << " " << cid << " " << oldoid
	   << " -> " << newoid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef oo = c->get_object(oldoid);
  if (!oo)
    return -ENOENT;
  ObjectRef no = c->get_or_create_object(newoid);
  used_bytes += oo->get_size() - no->get_size();
  no->clone(oo.get(), 0, oo->get_size(), 0);

  // take xattr and omap locks with std::lock()
  std::unique_lock<std::mutex>
      ox_lock(oo->xattr_mutex, std::defer_lock),
      nx_lock(no->xattr_mutex, std::defer_lock),
      oo_lock(oo->omap_mutex, std::defer_lock),
      no_lock(no->omap_mutex, std::defer_lock);
  std::lock(ox_lock, nx_lock, oo_lock, no_lock);

  no->omap_header = oo->omap_header;
  no->omap = oo->omap;
  no->xattr = oo->xattr;
  return 0;
}

int BuddyStore::_clone_range(const coll_t& cid, const ghobject_t& oldoid,
			   const ghobject_t& newoid,
			   uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
  dout(10) << __func__ << " " << cid << " "
	   << oldoid << " " << srcoff << "~" << len << " -> "
	   << newoid << " " << dstoff << "~" << len
	   << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef oo = c->get_object(oldoid);
  if (!oo)
    return -ENOENT;
  ObjectRef no = c->get_or_create_object(newoid);
  if (srcoff >= oo->get_size())
    return 0;
  if (srcoff + len >= oo->get_size())
    len = oo->get_size() - srcoff;

  const ssize_t old_size = no->get_size();
  no->clone(oo.get(), srcoff, len, dstoff);
  used_bytes += (no->get_size() - old_size);

  return len;
}

int BuddyStore::_omap_clear(const coll_t& cid, const ghobject_t &oid)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  o->omap.clear();
  o->omap_header.clear();
  return 0;
}

int BuddyStore::_omap_setkeys(const coll_t& cid, const ghobject_t &oid,
			    bufferlist& aset_bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  bufferlist::iterator p = aset_bl.begin();
  __u32 num;
  ::decode(num, p);
  while (num--) {
    string key;
    ::decode(key, p);
    ::decode(o->omap[key], p);
  }
  return 0;
}

int BuddyStore::_omap_rmkeys(const coll_t& cid, const ghobject_t &oid,
			   bufferlist& keys_bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  bufferlist::iterator p = keys_bl.begin();
  __u32 num;
  ::decode(num, p);
  while (num--) {
    string key;
    ::decode(key, p);
    o->omap.erase(key);
  }
  return 0;
}

int BuddyStore::_omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
			       const string& first, const string& last)
{
  dout(10) << __func__ << " " << cid << " " << oid << " " << first
	   << " " << last << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  map<string,bufferlist>::iterator p = o->omap.lower_bound(first);
  map<string,bufferlist>::iterator e = o->omap.lower_bound(last);
  o->omap.erase(p, e);
  return 0;
}

int BuddyStore::_omap_setheader(const coll_t& cid, const ghobject_t &oid,
			      const bufferlist &bl)
{
  dout(10) << __func__ << " " << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;

  ObjectRef o = c->get_object(oid);
  if (!o)
    return -ENOENT;
  std::lock_guard<std::mutex> lock(o->omap_mutex);
  o->omap_header = bl;
  return 0;
}

int BuddyStore::_create_collection(const coll_t& cid, int bits)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::WLocker l(coll_lock);
  auto result = coll_map.insert(std::make_pair(cid, CollectionRef()));
  if (!result.second){
    /////////
    //auto coll_info = coll_map.find(cid);
    //coll_info->second->bits = bits;

    return -EEXIST;
  }
  //result.first->second.reset(new Collection(cct, basedir, cid, data_directio));
  result.first->second.reset(new Collection(cct, basedir, cid));
  result.first->second->bits = bits;


#ifdef ALICIA
//  BuddyFileStore::BuddyFileObject* bdfo = bdfs.buddy_create_file_object(cid);
//  result.first->second->bdfo = bdfo;
#endif

#if 0
#ifdef ALICIA
#endif
#endif
  return 0;
}

int BuddyStore::_destroy_collection(const coll_t& cid)
{
  dout(10) << __func__ << " " << cid << dendl;
  RWLock::WLocker l(coll_lock);
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return -ENOENT;
  {
    RWLock::RLocker l2(cp->second->lock);
    if (!cp->second->object_map.empty())
      return -ENOTEMPTY;
    cp->second->exists = false;
  }
  used_bytes -= cp->second->used_bytes();
  coll_map.erase(cp);
#if 0
#ifdef ALICIA
  // 1. create coll data file 
  string fn;

  fn = path + cid.to_str();
  int r = ::unlink(fn.c_str());
  if (r < 0) {
    return -ENOENT;
  }
  dout(10) << __func__ << " " << "coll_fn : " << fn << " is destroyed "<< dendl;
#endif
#endif
  return 0;
}

int BuddyStore::_collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << cid << " " << ocid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  CollectionRef oc = get_collection(ocid);
  if (!oc)
    return -ENOENT;
  RWLock::WLocker l1(MIN(&(*c), &(*oc))->lock);
  RWLock::WLocker l2(MAX(&(*c), &(*oc))->lock);

  if (c->object_hash.count(oid))
    return -EEXIST;
  if (oc->object_hash.count(oid) == 0)
    return -ENOENT;
  ObjectRef o = oc->object_hash[oid];
  c->object_map[oid] = o;
  c->object_hash[oid] = o;
  return 0;
}

int BuddyStore::_collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
				      coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << " " << oldcid << " " << oldoid << " -> "
	   << cid << " " << oid << dendl;
  CollectionRef c = get_collection(cid);
  if (!c)
    return -ENOENT;
  CollectionRef oc = get_collection(oldcid);
  if (!oc)
    return -ENOENT;

  // note: c and oc may be the same
  assert(&(*c) == &(*oc));
  c->lock.get_write();

  int r = -EEXIST;
  if (c->object_hash.count(oid))
    goto out;
  r = -ENOENT;
  if (oc->object_hash.count(oldoid) == 0)
    goto out;
  {
    ObjectRef o = oc->object_hash[oldoid];
    c->object_map[oid] = o;
    c->object_hash[oid] = o;
    oc->object_map.erase(oldoid);
    oc->object_hash.erase(oldoid);
  }
  r = 0;
 out:
  c->lock.put_write();
  return r;
}

int BuddyStore::_split_collection(const coll_t& cid, uint32_t bits, uint32_t match,
				coll_t dest)
{
  dout(10) << __func__ << " " << cid << " " << bits << " " << match << " "
	   << dest << dendl;
  CollectionRef sc = get_collection(cid);
  if (!sc)
    return -ENOENT;
  CollectionRef dc = get_collection(dest);
  if (!dc)
    return -ENOENT;
  RWLock::WLocker l1(MIN(&(*sc), &(*dc))->lock);
  RWLock::WLocker l2(MAX(&(*sc), &(*dc))->lock);

  map<ghobject_t,ObjectRef>::iterator p = sc->object_map.begin();
  while (p != sc->object_map.end()) {
    if (p->first.match(bits, match)) {
      dout(20) << " moving " << p->first << dendl;
      dc->object_map.insert(make_pair(p->first, p->second));
      dc->object_hash.insert(make_pair(p->first, p->second));
      sc->object_hash.erase(p->first);
      sc->object_map.erase(p++);
    } else {
      ++p;
    }
  }

  sc->bits = bits;
  assert(dc->bits == (int)bits);

  return 0;
}

namespace {
struct BufferlistObject : public BuddyStore::Object {
  Spinlock mutex;

  bufferlist data;
//  size_t get_size() const override { return data.length(); }

  // EUNJI
  size_t data_size = 0;
  size_t get_size() const override { return data_size; }

  int read(uint64_t offset, uint64_t len, bufferlist &bl) override;
  int write(uint64_t offset, const bufferlist &bl) override;
  int clone(Object *src, uint64_t srcoff, uint64_t len,
            uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
//    ::encode(data, bl);
    ::encode(data_size, bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) override {
    DECODE_START(1, p);
    //::decode(data, p);
    ::decode(data_size, p);
    decode_base(p);
    DECODE_FINISH(p);
  }
};
}
// BufferlistObject
int BufferlistObject::read(uint64_t offset, uint64_t len,
                                     bufferlist &bl)
{

#ifdef HOLD_IN_MEMORY
  std::lock_guard<Spinlock> lock(mutex);
  bl.substr_of(data, offset, len);
  return bl.length();
#else
  return 0;
#endif
}

int BufferlistObject::write(uint64_t offset, const bufferlist &src)
{
/// 
// in-memory 에 들고 있는 대신에file 에서 찾아올 수 있는 매커니즘 구현 필요. 
// write 에서는 사실 할게 없음. 
// size 를 업데이트 해야함. 
  
  std::lock_guard<Spinlock> lock(mutex);

  unsigned len = src.length();

  if (data_size < offset + len)
    data_size = offset + len;

#ifdef HOLD_IN_MEMORY
  // before
  bufferlist newdata;
  if (get_size() >= offset) {
    newdata.substr_of(data, 0, offset);
  } else {
    if (get_size()) {
      newdata.substr_of(data, 0, get_size());
    }
    newdata.append_zero(offset - get_size());
  }

  newdata.append(src);

  // after
  if (get_size() > offset + len) {
    bufferlist tail;
    tail.substr_of(data, offset + len, get_size() - (offset + len));
    newdata.append(tail);
  }

  data.claim(newdata);
#endif
  return 0;
}

int BufferlistObject::clone(Object *src, uint64_t srcoff,
                                      uint64_t len, uint64_t dstoff)
{
  if(data_size < dstoff + len)
    data_size = dstoff + len;
  return 0;

#if 0
  auto srcbl = dynamic_cast<BufferlistObject*>(src);
  if (srcbl == nullptr)
    return -ENOTSUP;

  bufferlist bl;
  {
    std::lock_guard<Spinlock> lock(srcbl->mutex);
    if (srcoff == dstoff && len == src->get_size()) {
      data = srcbl->data;
      return 0;
    }
    bl.substr_of(srcbl->data, srcoff, len);
  }
  return write(dstoff, bl);
#endif

}

int BufferlistObject::truncate(uint64_t size)
{
  std::lock_guard<Spinlock> lock(mutex);

  data_size = size;

#if 0
  if (get_size() > size) {
    bufferlist bl;
    bl.substr_of(data, 0, size);
    data.claim(bl);
  } else if (get_size() == size) {
    // do nothing
  } else {
    data.append_zero(size - get_size());
  }
#endif
  return 0;
}

// PageSetObject

struct BuddyStore::PageSetObject : public Object {
  PageSet data;
  uint64_t data_len;
#if defined(__GLIBCXX__)
  // use a thread-local vector for the pages returned by PageSet, so we
  // can avoid allocations in read/write()
  static thread_local PageSet::page_vector tls_pages;
#endif

  explicit PageSetObject(size_t page_size) : data(page_size), data_len(0) {}

  size_t get_size() const override { return data_len; }

  int read(uint64_t offset, uint64_t len, bufferlist &bl) override;
  int write(uint64_t offset, const bufferlist &bl) override;
  int clone(Object *src, uint64_t srcoff, uint64_t len,
            uint64_t dstoff) override;
  int truncate(uint64_t offset) override;

  void encode(bufferlist& bl) const override {
    ENCODE_START(1, 1, bl);
    ::encode(data_len, bl);
    data.encode(bl);
    encode_base(bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) override {
    DECODE_START(1, p);
    ::decode(data_len, p);
    data.decode(p);
    decode_base(p);
    DECODE_FINISH(p);
  }
};

#if defined(__GLIBCXX__)
// use a thread-local vector for the pages returned by PageSet, so we
// can avoid allocations in read/write()
thread_local PageSet::page_vector BuddyStore::PageSetObject::tls_pages;
#define DEFINE_PAGE_VECTOR(name)
#else
#define DEFINE_PAGE_VECTOR(name) PageSet::page_vector name;
#endif

int BuddyStore::PageSetObject::read(uint64_t offset, uint64_t len, bufferlist& bl)
{
  const auto start = offset;
  const auto end = offset + len;
  auto remaining = len;

  DEFINE_PAGE_VECTOR(tls_pages);
  data.get_range(offset, len, tls_pages);

  // allocate a buffer for the data
  buffer::ptr buf(len);

  auto p = tls_pages.begin();
  while (remaining) {
    // no more pages in range
    if (p == tls_pages.end() || (*p)->offset >= end) {
      buf.zero(offset - start, remaining);
      break;
    }
    auto page = *p;

    // fill any holes between pages with zeroes
    if (page->offset > offset) {
      const auto count = std::min(remaining, page->offset - offset);
      buf.zero(offset - start, count);
      remaining -= count;
      offset = page->offset;
      if (!remaining)
        break;
    }

    // read from page
    const auto page_offset = offset - page->offset;
    const auto count = min(remaining, data.get_page_size() - page_offset);

    buf.copy_in(offset - start, count, page->data + page_offset);

    remaining -= count;
    offset += count;

    ++p;
  }

  tls_pages.clear(); // drop page refs

  bl.append(std::move(buf));
  return len;
}

int BuddyStore::PageSetObject::write(uint64_t offset, const bufferlist &src)
{
  unsigned len = src.length();

  DEFINE_PAGE_VECTOR(tls_pages);
  // make sure the page range is allocated
  data.alloc_range(offset, src.length(), tls_pages);

  auto page = tls_pages.begin();

  auto p = src.begin();
  while (len > 0) {
    unsigned page_offset = offset - (*page)->offset;
    unsigned pageoff = data.get_page_size() - page_offset;
    unsigned count = min(len, pageoff);
    p.copy(count, (*page)->data + page_offset);
    offset += count;
    len -= count;
    if (count == pageoff)
      ++page;
  }
  if (data_len < offset)
    data_len = offset;
  tls_pages.clear(); // drop page refs
  return 0;
}

int BuddyStore::PageSetObject::clone(Object *src, uint64_t srcoff,
                                   uint64_t len, uint64_t dstoff)
{
  const int64_t delta = dstoff - srcoff;

  auto &src_data = static_cast<PageSetObject*>(src)->data;
  const uint64_t src_page_size = src_data.get_page_size();

  auto &dst_data = data;
  const auto dst_page_size = dst_data.get_page_size();

  DEFINE_PAGE_VECTOR(tls_pages);
  PageSet::page_vector dst_pages;

  while (len) {
    // limit to 16 pages at a time so tls_pages doesn't balloon in size
    auto count = std::min(len, (uint64_t)src_page_size * 16);
    src_data.get_range(srcoff, count, tls_pages);

    // allocate the destination range
    // TODO: avoid allocating pages for holes in the source range
    dst_data.alloc_range(srcoff + delta, count, dst_pages);
    auto dst_iter = dst_pages.begin();

    for (auto &src_page : tls_pages) {
      auto sbegin = std::max(srcoff, src_page->offset);
      auto send = std::min(srcoff + count, src_page->offset + src_page_size);

      // zero-fill holes before src_page
      if (srcoff < sbegin) {
        while (dst_iter != dst_pages.end()) {
          auto &dst_page = *dst_iter;
          auto dbegin = std::max(srcoff + delta, dst_page->offset);
          auto dend = std::min(sbegin + delta, dst_page->offset + dst_page_size);
          std::fill(dst_page->data + dbegin - dst_page->offset,
                    dst_page->data + dend - dst_page->offset, 0);
          if (dend < dst_page->offset + dst_page_size)
            break;
          ++dst_iter;
        }
        const auto c = sbegin - srcoff;
        count -= c;
        len -= c;
      }

      // copy data from src page to dst pages
      while (dst_iter != dst_pages.end()) {
        auto &dst_page = *dst_iter;
        auto dbegin = std::max(sbegin + delta, dst_page->offset);
        auto dend = std::min(send + delta, dst_page->offset + dst_page_size);

        std::copy(src_page->data + (dbegin - delta) - src_page->offset,
                  src_page->data + (dend - delta) - src_page->offset,
                  dst_page->data + dbegin - dst_page->offset);
        if (dend < dst_page->offset + dst_page_size)
          break;
        ++dst_iter;
      }

      const auto c = send - sbegin;
      count -= c;
      len -= c;
      srcoff = send;
      dstoff = send + delta;
    }
    tls_pages.clear(); // drop page refs

    // zero-fill holes after the last src_page
    if (count > 0) {
      while (dst_iter != dst_pages.end()) {
        auto &dst_page = *dst_iter;
        auto dbegin = std::max(dstoff, dst_page->offset);
        auto dend = std::min(dstoff + count, dst_page->offset + dst_page_size);
        std::fill(dst_page->data + dbegin - dst_page->offset,
                  dst_page->data + dend - dst_page->offset, 0);
        ++dst_iter;
      }
      srcoff += count;
      dstoff += count;
      len -= count;
    }
    dst_pages.clear(); // drop page refs
  }

  // update object size
  if (data_len < dstoff)
    data_len = dstoff;
  return 0;
}

int BuddyStore::PageSetObject::truncate(uint64_t size)
{
  data.free_pages_after(size);
  data_len = size;

  const auto page_size = data.get_page_size();
  const auto page_offset = size & ~(page_size-1);
  if (page_offset == size)
    return 0;

  DEFINE_PAGE_VECTOR(tls_pages);
  // write zeroes to the rest of the last page
  data.get_range(page_offset, page_size, tls_pages);
  if (tls_pages.empty())
    return 0;

  auto page = tls_pages.begin();
  auto data = (*page)->data;
  std::fill(data + (size - page_offset), data + page_size, 0);
  tls_pages.clear(); // drop page ref
  return 0;
}


BuddyStore::ObjectRef BuddyStore::Collection::create_object() const {
  if (use_page_set)
    return new PageSetObject(cct->_conf->buddystore_page_size);
  return new BufferlistObject();
}



////--------------------

BuddyStore::Op *BuddyStore::build_op(vector<Transaction>& tls,
				   Context *onreadable,
				   Context *onreadable_sync,
				   Context *ondisk,
				   TrackedOpRef osd_op)
{

  dout(10) << __func__ << " transactions = " << tls.size() << dendl;
  uint64_t bytes = 0, ops = 0;
  for (vector<Transaction>::iterator p = tls.begin();
       p != tls.end();
       ++p) {
    bytes += (*p).get_num_bytes();
    ops += (*p).get_num_ops();
  }

  Op *o = new Op;
  o->start = ceph_clock_now();
  o->tls = std::move(tls);
  o->onreadable = onreadable;
  o->onreadable_sync = onreadable_sync;
  o->ondisk = ondisk;
  o->ops = ops;
  o->bytes = bytes;
  o->osd_op = osd_op;
  return o;
}


void BuddyStore::queue_op(OpSequencer *osr, Op *o)
{
  // queue op on sequencer, then queue sequencer for the threadpool,
  // so that regardless of which order the threads pick up the
  // sequencer, the op order will be preserved.

  osr->queue(o);

  if(logger){
    logger->inc(l_buddystore_ops);
    logger->inc(l_buddystore_bytes, o->bytes);
  }


  dout(5) << "queue_op " << o << " seq " << o->op
	  << " " << *osr
	  << " " << o->bytes << " bytes"
//	  << "   (queue has " << throttle_ops.get_current() << " ops and " << throttle_bytes.get_current() << " bytes)"
	  << dendl;
  op_wq.queue(osr);
}


void BuddyStore::queue_data_op(OpSequencer *osr, Op *o)
{
  // queue op on sequencer, then queue sequencer for the threadpool,
  // so that regardless of which order the threads pick up the
  // sequencer, the op order will be preserved.

  //osr->queue(o);
  osr->queue_data(o);

#if 0
  if(logger){
    logger->inc(l_buddystore_ops);
    logger->inc(l_buddystore_bytes, o->bytes);
  }
#endif
  dout(5) << "queue_data_op " << o << " seq " << o->op
	  << " " << *osr
	  << " " << o->bytes << " bytes"
//	  << "   (queue has " << throttle_ops.get_current() << " ops and " << throttle_bytes.get_current() << " bytes)"
	  << dendl;
  data_op_wq.queue(osr);
}


#if 0
void buddystore::queue_io_op(opsequencer *osr, op *o)
{
  dout(5) << "queue_io_op " << o << " seq " << o->op
	  << " " << *osr
	  << " " << o->bytes << " bytes"
	  << "   (queue has " << throttle_ops.get_current() << " ops and " << throttle_bytes.get_current() << " bytes)"
	  << dendl;
  op_wq.queue(osr);
}
#endif


#if 0
/******************************
 *  data write functions 
 *
 *  **************************/

void BuddyStore::start_data_write_thread()
{
  data_write_stop = false;
  //aio_stop = false;
  data_write_thread.create("journal_write");
}

void BuddyStore::stop_data_write_thread()
{
  // Do nothing if writer already stopped or never started
  if (!data_write_stop)
  {
    {
      Mutex::Locker p(data_writeq_lock);
      data_write_stop = true;
      data_writeq_cond.Signal();
    }
    data_write_thread.join();

    //data_write_sync();
  }
}

void BuddyStore::submit_data_write_entry()
{}

////////////////////////////
// data_write_entry()
////////////////////////////
void BuddyStore::data_write_thread_entry()
{

  dout(10) << "data_write_thread_entry start" << dendl;
  while (1) {

    Op* o;
    ///// sleep and wait .. 
    {
      Mutex::Locker locker(data_writeq_lock);
      if (data_writeq.empty()) {
	if (data_write_stop)
	  break;
	dout(20) << "data_write_thread_entry going to sleep" << dendl;

	data_writeq_cond.Wait(data_writeq_lock);
	dout(20) << "data_write_thread_entry woke up" << dendl;
	continue;
      }
      o = data_writeq.front();
      // unlocked 
    }

    // write 
    for(vector<buddy_iov_t>::iterator iovp = o->tls_iov.begin();
      iovp != o->tls_iov.end(); iovp++) {
    
      buddy_iov_t& iov = *iovp;
    
      dout(10) << "iov = " << iov << dendl;
      assert(iov.bytes == iov.data_bl.length());

      int r = iov.data_bl.write_file(iov.fname.c_str(), iov.foff, 0644);

      if (r) { // err
	ldout(cct, 10) << "Error in write "<< dendl;
      }
      iovp->data_bl.clear();
    } // end of buddy_iov_t loop 

    {
      Mutex::Locker locker(data_writeq_lock);
      data_writeq.pop_front();
    }
  } // end of while 
}

#if 0
void BuddyStore::_finish_io_op(OpSequencer *osr)
//void FileStore::_finish_op(OpSequencer *osr)
{

  // 문제는 여기선 op 를 알수가 없음. 
  // do_io_op 에서 마무리까지 해야하는듯. 
  // 이게 finish_io_op 는 순차적으로 호출이 되는듯. 
  // work queue 에서 그렇게 해줌. 
  // 그래서 jcount 조절하는거는 - 먼저 하고- 
  // 실제 q에서 뺴는건 여기서 하면 될듯. 

  // io 끝났으니까 체크하고- 
  // 만약.. 

  dout(5) << __func__ << o << " seq " << o->op << " " << *osr << " " << o->tls << dendl;

  int r = osr->dec_jcount(osr->op->op, 
   
   
    > 0){
    dout(5) << "stil in journaling .. jcount = " << dendl;
    return;
  }
  // this should queue in order because the journal does it's completions in order.
  queue_mem_op(osr, o);

  list<Context*> to_queue;
  osr->dequeue_journal(o->op, &to_queue);

  // do ondisk completions async, to prevent any onreadable_sync completions
  // getting blocked behind an ondisk completion.
  if (ondisk) {
    dout(10) << " queueing ondisk " << ondisk << dendl;
    ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(ondisk);
  }
  if (!to_queue.empty()) {
    ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(to_queue);
  }
}


#endif

#endif
int BuddyStore::_do_transactions(
  vector<Transaction> &tls,
  uint64_t op_seq,
  ThreadPool::TPHandle *handle)
{
  int trans_num = 0;

  for (vector<Transaction>::iterator p = tls.begin();
       p != tls.end();
       ++p, trans_num++) {
    _do_transaction(*p, op_seq, trans_num, handle);
    if (handle)
      handle->reset_tp_timeout();
  }

  return 0;
}

void BuddyStore::_do_op(OpSequencer *osr, ThreadPool::TPHandle &handle)
{

////////////// 이 부분은 잘 모르겄고.. 
#if 0
  if (!m_disable_wbthrottle) {
    wbthrottle.throttle();
  }
  // inject a stall?
  if (cct->_conf->filestore_inject_stall) {
    int orig = cct->_conf->filestore_inject_stall;
    dout(5) << "_do_op filestore_inject_stall " << orig << ", sleeping" << dendl;
    sleep(orig);
    cct->_conf->set_val("filestore_inject_stall", "0");
    dout(5) << "_do_op done stalling" << dendl;
  }
#endif
////////////////////

  /// queue 에서 꺼내서 start 까지는 무조건 atomic 해야함. 
  // 그런데 생각해보면.. 실제 transaction 실행하는 것도 순서대로 적용되어야 하긴 함. 
  // 그런데 또 다시 생각해보면. 겹치는게 없다면 굳이 순서대로 할필요는 없지. 
  // 아마 ceph 에서 이건 상윗단에서 알아서 처리한다고 가정함. 
  // 그래서 apply_lock 이 osr 에 있는거로군. 이제 알겠네. 
  // 물론. 동일한 osr 이라고 하더라도 겹치는게 없고, 저널링만 순차적으로 됐다면- 
  // 여기서는 parallel 하게 실행하도록 할수도 있을듯. 
  // 머. 성능향상은 minor 할것 같음. 
  osr->apply_lock.Lock();
  Op *o = osr->peek_queue();
  apply_manager.op_apply_start(o->op);

  dout(5) << "_do_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << " start" << dendl;
  int r = _do_transactions(o->tls, o->op, &handle);
  apply_manager.op_apply_finish(o->op);
  dout(10) << "_do_op " << o << " seq " << o->op << " r = " << r
	   << ", finisher " << o->onreadable << " " << o->onreadable_sync << dendl;

  o->tls.clear();

}

void BuddyStore::_finish_op(OpSequencer *osr)
{
  list<Context*> to_queue;
  Op *o = osr->dequeue(&to_queue);

  utime_t lat = ceph_clock_now();
  lat -= o->start;

  dout(5) << "_finish_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << " lat " << lat << dendl;
  osr->apply_lock.Unlock();  // locked in _do_op

  // called with tp lock held
  op_queue_release_throttle(o);

  // 실제 in-memory 까지 업데이트 된 시간 말함. 
  logger->tinc(l_buddystore_apply_latency, lat);

  if (o->onreadable_sync) {
    o->onreadable_sync->complete(0);
  }
  if (o->onreadable) {
    apply_finisher.queue(o->onreadable);
    //apply_finishers[osr->id % m_apply_finisher_num]->queue(o->onreadable);
  }

  // 이 경우는 지금은 없는듯. 
  if (!to_queue.empty()) {
    apply_finisher.queue(to_queue);
    //apply_finishers[osr->id % m_apply_finisher_num]->queue(to_queue);
  }
  delete o;


  ///// dump for testing 
  //dump_logger();
}


void BuddyStore::_do_data_op(OpSequencer *osr, ThreadPool::TPHandle &handle)
{

////////////// 이 부분은 잘 모르겄고.. 
#if 0
  if (!m_disable_wbthrottle) {
    wbthrottle.throttle();
  }
  // inject a stall?
  if (cct->_conf->filestore_inject_stall) {
    int orig = cct->_conf->filestore_inject_stall;
    dout(5) << "_do_op filestore_inject_stall " << orig << ", sleeping" << dendl;
    sleep(orig);
    cct->_conf->set_val("filestore_inject_stall", "0");
    dout(5) << "_do_op done stalling" << dendl;
  }
#endif
////////////////////

  /// queue 에서 꺼내서 start 까지는 무조건 atomic 해야함. 
  // 그런데 생각해보면.. 실제 transaction 실행하는 것도 순서대로 적용되어야 하긴 함. 
  // 그런데 또 다시 생각해보면. 겹치는게 없다면 굳이 순서대로 할필요는 없지. 
  // 아마 ceph 에서 이건 상윗단에서 알아서 처리한다고 가정함. 
  // 그래서 apply_lock 이 osr 에 있는거로군. 이제 알겠네. 
  // 물론. 동일한 osr 이라고 하더라도 겹치는게 없고, 저널링만 순차적으로 됐다면- 
  // 여기서는 parallel 하게 실행하도록 할수도 있을듯. 
  // 머. 성능향상은 minor 할것 같음. 
  osr->data_lock.Lock();
  Op *o = osr->peek_queue_data();

  dout(5) << __func__ << " seq " << o->op << " iov " << o->tls_iov.size() << dendl;
    //" " << *osr << "/" << osr->parent << " start" << dendl;

  BuddyHashIndexFile* data_file;

  // write 
  for(vector<buddy_iov_t>::iterator iovp = o->tls_iov.begin();
      iovp != o->tls_iov.end(); iovp++) {
    
      buddy_iov_t& iov = *iovp;
    
      dout(10) << "iov = " << iov << dendl;
      assert(iov.bytes == iov.data_bl.length());

      ////// 
      // 이 부분에서 write_file 로 가지말고 
      // BuddyHashFile 의 write 함수를 구현해서 보내는게 맞는듯. 
      // 대신 BuddyHashFile 은 파일을 open 한 상태로 유지하고.
      // @ param : buddy_iov_t 만 보내면. 알아서 쓸듯. 
      // 열려있는 fd 로 write_Fd (iov.foff ) 이렇게 하면 될듯. 
      // direct io 인 경우에는 rebuild 한번 해주고.
      // 아니면 sync 도 해주고 (이건 마지막에 한번이니까 밖에서 불러야할듯) 
      // // ooff 는 해당 object 에서 어디인지를 나타냄. 
      // `
      //
  
      //CollectionRef c = get_collection(*(iov.cid));
      //int r = c->data_file.write_fd(iov.data_bl, iov.foff);

      {
	RWLock::RLocker l(coll_file_lock);

	ceph::unordered_map<coll_t, BuddyHashIndexFile*>::iterator p = coll_file_map.find(*(iov.cid));
	if(p != coll_file_map.end()){
	  data_file = p->second;
	} else {
	  dout(5) << __func__ << " Failed to find collection data file cid " << *(iov.cid) << dendl;
	  continue;
	}
      }

      //int r = data_file->write_fd(iov.data_bl, iov.foff);
      int r = data_file->write_fd(iov.data_bl, 0);

      if (r) { // err
	ldout(cct, 10) << __func__ << "Error in write ret = "<< r << dendl;
      } else {
	ldout(cct, 10) << __func__ << "Success in write "<< dendl;
      }


      if(logger){
	//logger->inc(l_buddystore_ops);
	logger->inc(l_buddystore_data_wr_bytes, iov.data_bl.length());
      }


      //int r = iov.data_bl.write_file(iov.fname.c_str(), iov.foff, 0644);
      
      // sync 하는 부분은 unlock 다음에 하는게 좋을듯. 
      // 사실 지금 do_op 랑 finish_op 를 나눠놓았는데- 합쳐도 될 것 같음.
      if (!data_directio && data_sync)
	data_file->sync();

      iovp->data_bl.clear();
  } // end of buddy_iov_t loop 

  o->tls_iov.clear();

}

void BuddyStore::_finish_data_op(OpSequencer *osr)
{

  Op *o = osr->peek_queue_data();


  /// time 
  utime_t lat = ceph_clock_now();
  lat -= o->start;

  if (logger) {
    logger->tinc(l_buddystore_data_wr_latency, lat);
  }

  dout(5) << __func__ << " data_write complete lat " << lat << dendl; 

  // dec jcount and check 
  int r = osr->dec_jcount(o->op);
  
  dout(10) << __func__ << " seq " << o->op <<" jcount = " << r << dendl;  

  // jcount > 0 : still waiting 
  if (r > 0) {
    // dequeue 
    list<Context*> to_queue;
    osr->dequeue_data(&to_queue);
    osr->data_lock.Unlock();
 
    return;
  }

  // verification   
  Op* jo = osr->peek_queue_journal();
  if(jo)
    assert(o->op <= jo->op);

  queue_op(osr, o);

  // dequeue 
  list<Context*> to_queue;
  osr->dequeue_data(&to_queue);

  osr->data_lock.Unlock();

   
  // called with tp lock held
  data_op_queue_release_throttle(o);

  // do ondisk completions async, to prevent any onreadable_sync completions
  // getting blocked behind an ondisk completion.
  if (o->ondisk) {
    Mutex::Locker locker(ondisk_finisher_lock);
    dout(20) << __func__ << "on finisher_queue" << dendl;
    ondisk_finisher.queue(o->ondisk);
  //  dout(10) << " queueing ondisk " << ondisk << dendl;
    //ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(ondisk);
  }
  if (!to_queue.empty()) {
    Mutex::Locker locker(ondisk_finisher_lock);
    dout(20) << "to_queue is not empty" << dendl;
    ondisk_finisher.queue(to_queue);
    //ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(to_queue);
  }

  if (logger) {
    logger->tinc(l_buddystore_journal_all_latency, lat);
  }
  dout(5) << __func__ << " journal_all complete lat " << lat << dendl; 
}


////----------------------------
// OpSequencer functions 
// -----------------------------


bool BuddyStore::OpSequencer::_get_max_uncompleted(
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

  if (!dq.empty() && dq.back()->op > *seq)
    *seq = dq.back()->op;


  return false;

} /// @returns true if both queues are empty


bool BuddyStore::OpSequencer::_get_min_uncompleted(
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

  if (!dq.empty() && dq.front()->op < *seq)
    *seq = dq.front()->op;


  return false;

} /// @returns true if both queues are empty


  
void BuddyStore::OpSequencer::_wake_flush_waiters(list<Context*> *to_queue) {
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
    void BuddyStore::OpSequencer::queue_journal(uint64_t s) {
      Mutex::Locker l(qlock);
      jq.push_back(s);
    }
    void BuddyStore::OpSequencer::dequeue_journal(list<Context*> *to_queue) {
      Mutex::Locker l(qlock);
      //assert(jcount[s] == 0);
      jq.pop_front();
      cond.Signal();
      _wake_flush_waiters(to_queue);
    }
#endif

void BuddyStore::OpSequencer::queue_journal(Op *o) {

  Mutex::Locker l(qlock);
  jq.push_back(o);

}

BuddyStore::Op * BuddyStore::OpSequencer::peek_queue_journal() {
  Mutex::Locker l(qlock);
  return jq.front();
}

BuddyStore::Op * BuddyStore::OpSequencer::dequeue_journal(list<Context*> *to_queue) {
  assert(to_queue);
//      assert(data_lock.is_locked());
  Mutex::Locker l(qlock);
  Op *o = jq.front();
 
//  assert(jcount[o->op] == 0);
//  jcount.erase(o->op);

  jq.pop_front();
  cond.Signal();

  _wake_flush_waiters(to_queue);
  return o;
}

void BuddyStore::OpSequencer::queue_data(Op *o) {

  Mutex::Locker l(qlock);
  dq.push_back(o);

}

BuddyStore::Op * BuddyStore::OpSequencer::peek_queue_data() {
  Mutex::Locker l(qlock);
// 이건 나중에 추가. verification 안할때. 
//  assert(data_lock.is_locked());
  return dq.front();
}

BuddyStore::Op * BuddyStore::OpSequencer::dequeue_data(list<Context*> *to_queue) {
  assert(to_queue);
  assert(data_lock.is_locked());

  Mutex::Locker l(qlock);
  Op *o = dq.front();
 
//  assert(jcount[o->op] == 0);
//  jcount.erase(o->op);

  dq.pop_front();
  cond.Signal();

  _wake_flush_waiters(to_queue);
  return o;
}


bool BuddyStore::OpSequencer::get_max_uncompleted(
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

void BuddyStore::OpSequencer::set_jcount(uint64_t seq, int count){  
  Mutex::Locker l(qlock);
  //jcount[seq] = count;  
  auto result = jcount.insert(make_pair(seq, count));
  assert(result.second);
  dout(10) << __func__ << " jcount = " << jcount[seq] << dendl;
}

    //int dec_jcount(uint64_t seq, Context* c, list<Context*> to_queue){
    //int dec_jcount(uint64_t seq, Context* c){
    
int BuddyStore::OpSequencer::dec_jcount(uint64_t seq){

  dout(10) << __func__ << " jcount = " << jcount[seq] << dendl;
  Mutex::Locker l(qlock);
  assert(jcount[seq] > 0);
  jcount[seq]--;

  if(jcount[seq] == 0){
    jcount.erase(seq);
    return 0;
  }
      
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

void BuddyStore::OpSequencer::queue(Op *o) {
  Mutex::Locker l(qlock);
  q.push_back(o);
}

BuddyStore::Op * BuddyStore::OpSequencer::peek_queue() {
  Mutex::Locker l(qlock);
  assert(apply_lock.is_locked());
  return q.front();
}


BuddyStore::Op * BuddyStore::OpSequencer::dequeue(list<Context*> *to_queue) {

  assert(to_queue);
  assert(apply_lock.is_locked());
  Mutex::Locker l(qlock);
  Op *o = q.front();
  q.pop_front();
  cond.Signal();

  _wake_flush_waiters(to_queue);

  return o;
}

void BuddyStore::OpSequencer::flush() {
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
    
bool BuddyStore::OpSequencer::flush_commit(Context *c) {

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

void BuddyStore::dump_logger()
{
  Formatter *f = Formatter::create("json-pretty");
  dump_perf_counters(f);
  
  dout(5) << "dump logger:";
  f->flush(*_dout);
  *_dout << dendl;

  delete f;
}
