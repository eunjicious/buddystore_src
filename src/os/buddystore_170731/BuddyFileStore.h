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


#ifndef CEPH_BUDDYFILESTORE_H
#define CEPH_BUDDYFILESTORE_H

//
//#include <mutex>
//#include <boost/intrusive_ptr.hpp>
//
//#include "include/unordered_map.h"
//#include "include/memory.h"
//#include "include/Spinlock.h"
//#include "common/Finisher.h"
//#include "common/RefCountedObj.h"
//#include "common/RWLock.h"
//#include "os/ObjectStore.h"
//#include "PageSet.h"
//#include "include/assert.h"
//
////// file io 
//#include <unistd.h>
//#include <stdlib.h>
//#include <sys/types.h>
//#include <sys/stat.h>
//#include <fcntl.h>
//#include <sys/file.h>
//#include <errno.h>
//#include <dirent.h>
//#include <sys/ioctl.h>
//
#include "include/compat.h"
#if defined(__linux__)
#include <linux/fs.h>
#endif

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




#define BUDDY_FILE_BSIZE_BITS 12


/// 위의 클래스를 BuddyFileStore 안에 넣어도 되나. 

///////////
class BuddyFileStore {

public:
  typedef enum { 
    none = 0,
    data = 1, 
    xattr, 
    omap_header, 
    omap_keys, 
  } comp_t;

  // Location of each elements of an object 
  struct buddy_omap_entry_t {
    comp_t type; // data, xattr, omap_header, omap_keys
    uint64_t ooff;
    uint64_t foff;
    uint64_t used_bytes;
    uint64_t alloc_bytes;

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);

    explicit buddy_omap_entry_t(comp_t t, uint64_t oo, uint64_t fo, uint64_t ubytes, uint64_t abytes){
      type = t;
      ooff = oo;
      foff = fo;
      used_bytes = ubytes;
      alloc_bytes = abytes;
    }
  };
  WRITE_CLASS_ENCODER(buddy_omap_entry_t)

  // a list of bufferlist with location information 
  struct buddy_buffer_head_t {
     string fname;
     uint64_t foff;
     uint64_t fbytes;
     bufferlist data_bl; 

     void encode(bufferlist& bl) const;
     void decode(bufferlist::iterator& bl);
  };
  WRITE_CLASS_ENCODER(buddy_buffer_head_t)

  // super block 
  struct BuddySuperblock {
  public:
    uint64_t coll_map_root_off; 
    uint64_t timestamp;
    uint64_t last_commit_pos;
  };

  struct buddy_log_entry_t {
    coll_t cid;
    ghobject_t oid;
    __le32 type;
    bufferlist arg_bl;
 
    // void op_encode(const coll_t& cid, const ghobject_t& oid, const __le32 op_type){
     
    void encode(bufferlist& log_bl){
      ENCODE_START(1, 1, log_bl);
      ::encode(cid, log_bl);
      ::encode(oid, log_bl);
      ::encode(type, log_bl);
      ::encode(arg_bl, log_bl);
      ENCODE_FINISH(log_bl);
    } 
     
    void decode(bufferlist::iterator& p){
      DECODE_START(1, p);
      ::decode(cid, p);
      ::decode(oid, p);
      ::decode(type, p);
      ::decode(arg_bl, p);
      DECODE_FINISH(p);
    } 

    explicit buddy_log_entry_t(const coll_t& c, const ghobject_t& o, __le32 t, bufferlist& bl)
    {
      cid = c;
      oid = o;
      type = t;
      arg_bl = bl;
    } 

    buddy_log_entry_t(){}
    ~buddy_log_entry_t(){}
  }; // end of struct BuddyOp

  /*****************************
   * BuddyFileObject
   *
   * A BuddyFileObject represetns a file for each collection. 
   *
   ******************************/
  class BuddyFileObject {

  protected:
    // collectionRef 에 함께 생성되니까. coll_t 갖고 있음. 
    uint64_t pos;
    int fd;
    vector<char> fr_bitmap;
    vector<char> sh_bitmap;
    buddy_omap_entry_t* last_bmi;


  public:
    string fn;
    void set_pos (const uint64_t value){ pos = value;}
    uint64_t get_pos (){return pos;}



    // map info 
    map<ghobject_t, map<comp_t, map<uint64_t, buddy_omap_entry_t>>> buddy_omap;

    int insert_bmi (const ghobject_t& oid, const buddy_omap_entry_t& bmi);
    int get_fspace (comp_t type, const ghobject_t& oid, const uint64_t ooff, 
	const uint64_t len, uint64_t& foff);

    // constructor 
    explicit BuddyFileObject(coll_t& cid) { 
      pos = 0;
      fn = cid.c_str();  // 여기서부터 하기.
    }
    BuddyFileObject(){}
    ~BuddyFileObject (){}
    }; // class BuddyFileObject
 

  /*********************************
  * BuddyTransaction 
  *
  * A BuddyTransaction represents a bunch of bufferlist to be written to 
  * multiple files. Each bufferlist holds the location information where they are going to be
  * recorded in a form of pair <fname, offset>
  *
  **********************************/ 



  class BuddyTransaction {

  public:
//    vector<???> gb_list; // garbage 
//
    bufferlist log_bl; // metadata log and checksum. flush in commit. 
    vector<buddy_buffer_head_t> bh_list; // to be flushed in commit 


    // member functinos 

    int prepare_commit();
    int do_commit();
    int end_commit();


    BuddyTransaction () {}
    ~BuddyTransaction () {}

  };

/********************
 * member
 ********************/

  map<coll_t, vector<buddy_omap_entry_t>> buddy_coll_map; // sorted map - store coll_t instead of cid only.    


  // constructor and destructor 
  BuddyFileStore (CephContext* cct, const string& path) {}
  ~BuddyFileStore () {}



};

#endif
