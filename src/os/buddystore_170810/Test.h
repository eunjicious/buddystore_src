//#define EUNJI
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


#ifdef EUNJI_V1
#include "BuddyFileStore.h"
#endif

#ifdef EUNJI
#include "BuddyJournal.h"
#include "Test.h"
//#include "BuddyFileStore.h"
#endif

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
//#include <linux/fs.h>
#endif

#define dout_context cct

enum {
  l_buddystore_first = 84000,
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

//class BuddyStore : public ObjectStore, public Test {
class BuddyStore : public ObjectStore {
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
//#ifdef EUNJI_V1
//  struct Collection : public CollectionImpl, public BuddyFileObject {
//#else
  struct Collection : public CollectionImpl {
//#endif
    coll_t cid;
    int bits;
    CephContext *cct;
    bool use_page_set;
    ceph::unordered_map<ghobject_t, ObjectRef> object_hash;  ///< for lookup
    map<ghobject_t, ObjectRef> object_map;        ///< for iteration
    map<string,bufferptr> xattr;
    RWLock lock;   ///< for object_{map,hash}
    bool exists;

    typedef boost::intrusive_ptr<Collection> Ref;
    friend void intrusive_ptr_add_ref(Collection *c) { c->get(); }
    friend void intrusive_ptr_release(Collection *c) { c->put(); }

#ifdef EUNJI_V1
    BuddyFileStore::BuddyCollection* bd_coll;
#endif

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

#if 0
#ifdef EUNJI_V1
    explicit Collection(CephContext *cct, coll_t c)
      : 
	cid(c),
	cct(cct),
	use_page_set(cct->_conf->buddystore_page_set),
        lock("BuddyStore::Collection::lock", true, false),
	exists(true),
	bd_coll(cct, c) {}
#else
#endif
#endif
    explicit Collection(CephContext *cct, coll_t c)
      : cid(c),
	cct(cct),
	use_page_set(cct->_conf->buddystore_page_set),
        lock("BuddyStore::Collection::lock", true, false),
	exists(true) {}
	  //bd_coll = bdfs.buddy_create_file_object(cct, c);
//#endif
  };
  typedef Collection::Ref CollectionRef;

private:
  class OmapIteratorImpl;


  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  RWLock coll_lock;    ///< rwlock to protect coll_map

  CollectionRef get_collection(const coll_t& cid);

  Finisher finisher;

#ifdef EUNJI_V1
  BuddyFileStore bdfs; 
#endif

  uint64_t used_bytes;

  void _do_transaction(Transaction& t);

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
  BuddyStore(CephContext *cct, const string& path)
    : ObjectStore(cct, path),
      coll_lock("BuddyStore::coll_lock"),
      finisher(cct),
#ifdef EUNJI_V1
      //BuddyFileStore(cct, path),
      bdfs(cct, path),
#endif
      used_bytes(0) {}
  ~BuddyStore() override { }

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
