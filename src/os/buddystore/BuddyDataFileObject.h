
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


#ifndef CEPH_BUDDYDATAFILEOBJECT_H
#define CEPH_BUDDYDATAFILEOBJECT_H

#if 0
#include "acconfig.h"

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#endif

#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "common/RWLock.h"
//#include "BuddyStore.h"
#include "include/compat.h"

#include "buddy_types.h"

/*****************************
 * BuddyHashIndexFile
 *
 * A BuddyHashIndexFile represetns a file for each collection. 
 * 이 구조체는 collection 이 자신의 collection file 을 관리하기 위해 
 * 필요한 메타정보를 저장하는 것임. 주기적으로 플러쉬 해주어야 함. 
 * collection 의 파일 이름 (즉, 실제 object 들을 담는) 은 
 * fname = basedir + "/" cid.to_str() 으로 구할수 있고 
 * 그 뒤에 meta 를 붙여서 해당 파일에 저장?? 
 * 아니지.. 
 *
 ******************************/

class BuddyDataFileObject {

  public:
    CephContext* cct;
    //coll_t cid; 

    // file information 
    string fname;
    bool directio;
    bool preallocation; 
    int type;
    //int dfd;

  //private:
  protected:
    // space management 
    uint64_t max_fbytes;
    uint64_t used_fbytes;

  public:
    //BitMap shadow_bitmap;
    //map<off_t, buddy_hindex_t> free_index_map;
    
    //RWLock lock; // hash_index_map lock 
    //map<off_t, buddy_hindex_t> hash_index_map;


    virtual int create_or_open_file(int flag) = 0;
    virtual int delete_file() = 0;
    virtual int close_file() = 0;

    virtual int alloc_space(int type, const ghobject_t& oid, const off_t ooff, const ssize_t bytes, 
      vector<buddy_iov_t>& iov) = 0;
    virtual int get_space_info(const ghobject_t& oid, const off_t ooff, const ssize_t bytes,
       vector<buddy_iov_t>& iov) = 0;
    // @return: -1 on fail 

    virtual int release_space(const ghobject_t& oid) = 0;
#if 0
    virtual int truncate_space(const ghobject_t& oid, ssize_t size) = 0; 

    virtual int clone_space(const ghobject_t& ooid, const ghobject_t& noid, vector<buddy_iov_t>& iov) = 0; 
    virtual int clone_space(const ghobject_t& ooid, const ghobject_t& noid, off_t srcoff, size_t bytes, 
	off_t dstoff, vector<buddy_iov_t>& iov) = 0;
#endif
    virtual int write_fd(bufferlist& bl, uint64_t foff) = 0;
    virtual void sync() = 0;

    //int read_fd(const ghobject_t& oid, bufferlist& bl, uint64_t foff, ssize_t size) = 0;
    virtual int read_fd(bufferlist& bl, uint64_t foff, size_t size) = 0;

    virtual int preallocate(uint64_t offset, size_t len) = 0;
//    int punchhole(offset, size) = 0;
 
#if 0 
    virtual void dump_hash_index_map() = 0;
    virtual void _dump_hash_index_map() = 0;

    // store HashIndexFile info persistently
    // - hash_index_map 
    // - shadow bitmap 
    // - max_fbytes, used_fbytes
    virtual void encode(bufferlist& bl) const {} 
//    void encode(bufferlist& bl) const 
//    {
//      ENCODE_START(1, 1, bl);
//      ::encode(hash_index_map, bl);
//      ::encode(max_fbytes, bl);
//      ::encode(used_fbytes, bl);
//      ENCODE_FINISH(bl);
//    }
    virtual void decode(bufferlist::iterator& p) {}
//    void decode(bufferlist::iterator& p)
//    {
//      DECODE_START(1, p);
//      ::decode(hash_index_map, p);
//      ::decode(max_fbytes, p);
//      ::decode(used_fbytes, p);
//      DECODE_FINISH(p);
//    }
#endif
    // constructor 
    //explicit 
    BuddyDataFileObject (CephContext* cct_, const coll_t& cid_, 
	string fname_, bool dio=true, bool prealloc=true): 
      cct(cct_),
      cid(cid_),
      fname(fname_),
      directio(dio),
      preallocation(prealloc) {
      //lock("BuddyDataFileObject::lock") {

      //max_fbytes= std::numeric_limits<uint64_t>::max();
      //max_fbytes = (1UL << 44) - 4096; // 16TB
      //used_fbytes = 0;

      //create_or_open_file(0);
    }
    BuddyDataFileObject (CephContext* cct_, const coll_t& cid_, 
	string fname_, bool dio=true, bool prealloc=true): 
      cct(cct_),
      cid(cid_),
      fname(fname_),
      directio(dio),
      preallocation(prealloc) {
      //lock("BuddyDataFileObject::lock") {

      //max_fbytes= std::numeric_limits<uint64_t>::max();
      //max_fbytes = (1UL << 44) - 4096; // 16TB
      //used_fbytes = 0;

      //create_or_open_file(0);
    }
    //BuddyHashIndexFile() {}
    //virtual ~BuddyDataFileObject(){}
#if 0
//  private: 
  protected:
    static uint32_t _reverse_bits(uint32_t v) {
      if (v == 0)
      
	return v;
    
      // reverse bits
      // swap odd and even bits
      v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
      // swap consecutive pairs
      v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
      // swap nibbles ...
      v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
      // swap bytes
      v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
      // swap 2-byte long pairs
      v = ( v >> 16             ) | ( v               << 16);
      return v;
    }

    off_t round_up(off_t v){
      return ((v >> BUDDY_FALLOC_SIZE_BITS)+1) << BUDDY_FALLOC_SIZE_BITS;
    }


    off_t round_down(off_t v){
      return (v >> BUDDY_FALLOC_SIZE_BITS) << BUDDY_FALLOC_SIZE_BITS;
    }

    off_t hash_to_hoff(const ghobject_t& oid){
      uint64_t bnum = (uint64_t)_reverse_bits(oid.hobj.get_hash());
      return bnum << BUDDY_FALLOC_SIZE_BITS; // hash(blk num) to bytes 
    }
    
    off_t get_hash(const ghobject_t& oid){
      return (uint64_t)_reverse_bits(oid.hobj.get_hash());
    }
#endif

}; // class BuddyHashIndexFile

#endif
