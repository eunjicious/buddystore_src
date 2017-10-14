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

#include "common/debug.h"
#include "common/TrackedOp.h"
#include "include/Context.h"
#include "common/safe_io.h"
#include "BitMap.h"

#define BUDDY_FILE_BSIZE_BITS 12
#define BUDDY_FILE_BSIZE 1 << 12
//
//#define dout_context cct
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddyfilestore(" << basedir << ") " 

class CephContext;

using std::vector;
using std::string;
using std::map;

namespace ceph {
  class Formatter;
}

/// 위의 클래스를 BuddyFileStore 안에 넣어도 되나. 


// Location of each elements of an object 
struct buddy_index_t {
  ghobject_t oid;
  int type; // data, xattr, omap_header, omap_keys
  uint64_t ooff;
  uint64_t foff;
  size_t used_bytes;
  size_t alloc_bytes;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);

  friend ostream& operator<<(ostream& out, const buddy_index_t& o);
  friend bool operator < (const buddy_index_t& b1, const buddy_index_t& b2);
  friend bool operator > (const buddy_index_t& b1, const buddy_index_t& b2);

  explicit buddy_index_t(ghobject_t oid_, int t, uint64_t oo, uint64_t fo, uint64_t ubytes, uint64_t abytes): 
    oid(oid_) {
    type = t;
    ooff = oo;
    foff = fo;
    used_bytes = ubytes;
    alloc_bytes = abytes;
  }

  buddy_index_t(){}
  ~buddy_index_t(){}
};
WRITE_CLASS_ENCODER(buddy_index_t)


///////////
class BuddyFileStore {

public:
  CephContext* cct;
  string basedir;

  void ldout_test();

//
//  typedef enum { 
//    none = 0,
//    data = 1, 
//    xattr, 
//    omap_header, 
//    omap_keys, 
//  } comp_t;
//

  const int none_t = 0;
  const int data_t = 1;
  const int xattr_t = 2;
  const int omap_header = 3;
  const int omap_keys = 4;
  // a list of bufferlist with location information 
  struct buddy_buffer_head_t {
     string fname;
     uint64_t foff;
     uint64_t fbytes;
     bufferlist data_bl; 

     void encode(bufferlist& bl) const;
     void decode(bufferlist::iterator& bl);

     friend ostream& operator<<(ostream& out, const buddy_buffer_head_t& o);

     explicit buddy_buffer_head_t(string fn, uint64_t o, uint64_t b){
       fname = fn;
       foff = o;
       fbytes = b;
     }
     buddy_buffer_head_t() : fname("init"), foff(0), fbytes(0){}
     ~buddy_buffer_head_t(){}

  };
  WRITE_CLASS_ENCODER(buddy_buffer_head_t)


  // super block 
  class buddy_sb_t {
  public:
    string fname;
    uint64_t last_cp_off; // coll_map 
    size_t last_cp_len; // coll_map size
    uint64_t last_cp_coll_off;
    size_t last_cp_coll_len;
    uint64_t last_cp_version;
    // uint64_t checksum;
    
    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& p);

    friend ostream& operator<<(ostream& out, const buddy_sb_t& o);

    explicit buddy_sb_t(string basedir_, string fname_){
      fname = basedir_ + "/" + fname_;
      last_cp_off = 0;
      last_cp_len = 0;
      last_cp_coll_off = 0;
      last_cp_coll_len = 0;
      last_cp_version = 0;
    }
    buddy_sb_t(){}
    ~buddy_sb_t(){}
  };
  WRITE_CLASS_ENCODER(buddy_sb_t)

  buddy_sb_t buddy_sb;
  int write_buddy_sb();
  int read_buddy_sb();


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

    explicit buddy_log_entry_t(const coll_t& c, const ghobject_t& o, __le32 t)
    {
      cid = c;
      oid = o;
      type = t;
    } 

    buddy_log_entry_t(){}
    ~buddy_log_entry_t(){}
  }; // end of struct BuddyOp

  /*****************************
   * BuddyLogFileObject
   *
   * A BuddyLogFileObject represetns a file for each collection. 
   *
   ******************************/
  class BuddyLogFileObject {

  public:
    CephContext* cct;
    string basedir;
    string fname;
    uint64_t max_size;

    uint64_t head_pos;
    uint64_t tail_pos;
    uint64_t commit_pos; // dont need? 

    int fd;


    int alloc_log_space (const uint64_t ooff, 
	const uint64_t len, uint64_t& foff, uint64_t& alloc_bytes);


    // constructor 
    explicit BuddyLogFileObject(CephContext* cct_, string basedir_, string fname_) : cct(cct_)
    {
      basedir = basedir_;
      fname = basedir + "/"+ fname_;  // 여기서부터 하기.
      max_size = std::numeric_limits<uint64_t>::max();
    }
    BuddyLogFileObject() {}
    ~BuddyLogFileObject(){}
    
  }; // class BuddyLogFileObject

  BuddyLogFileObject buddy_meta_lfo; // object_map, coll_map 
  BuddyLogFileObject buddy_cmt_lfo; // checksum, op logging 

  /*****************************
   * BuddyCollFileObject
   *
   * A BuddyCollFileObject represetns a file for each collection. 
   * 이 구조체는 collection 이 자신의 collection file 을 관리하기 위해 
   * 필요한 메타정보를 저장하는 것임. 주기적으로 플러쉬 해주어야 함. 
   * collection 의 파일 이름 (즉, 실제 object 들을 담는) 은 
   * fname = basedir + "/" cid.to_str() 으로 구할수 있고 
   * 그 뒤에 meta 를 붙여서 해당 파일에 저장?? 
   * 아니지.. 
   *
   ******************************/
  class BuddyCollFileObject {

    CephContext* cct;
    coll_t cid; // 저장해야 하지만. 이건 외부에서 저장한다고 치고.  

  public:
    string basedir;
    string fname;

    uint64_t tail_pos; // 로그로 할당한다고 가정할 때 끝. 나중엔 bitmap 으로 처리해야할듯..
    buddy_index_t* last_alloc_oindex;

    //BitMap alloc_bitmap;
    //BitMap shadow_bitmap;
    //vector<char> fr_bitmap;
    //vector<char> sh_bitmap;

    // map info :: oid - type - start_offset  
    map<ghobject_t, map<int, map<uint64_t, buddy_index_t>>> buddy_object_index;
    set<buddy_index_t> buddy_object_map;

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& p);

    ////////////////////////////

    // member functions 
    int delete_object_index (int type, const ghobject_t& oid, const uint64_t ooff, const uint64_t len);

    int insert_object_index (const ghobject_t& oid, const buddy_index_t& oindex);
    int alloc_space (int type, const ghobject_t& oid, const uint64_t ooff, 
	const uint64_t len, uint64_t& foff, uint64_t& alloc_bytes);

    // constructor 
    explicit BuddyCollFileObject(CephContext* cct_, string basedir_, const coll_t& cid_) : 
      cct(cct_), cid(cid_) {
      tail_pos = 0;
      basedir = basedir_;
      fname = basedir + "/"+ cid.to_str() + "_bd";  // 여기서부터 하기.
      last_alloc_oindex = NULL;
    }
    BuddyCollFileObject() {}
    ~BuddyCollFileObject(){}
    
  }; // class BuddyCollFileObject


 //mkcoll 할때 FileObject 를 등록해야 할듯..   
  //map<coll_t, vector<buddy_index_t>> buddy_coll_map; 
  map<coll_t, BuddyCollFileObject> buddy_coll_fo;
  
  void stat_buddy_coll_fo()
  {
    ldout(cct, 10) << __func__ << " coll_fo size = " << buddy_coll_fo.size() << dendl;
    for (map<coll_t, BuddyCollFileObject>::iterator p = buddy_coll_fo.begin(); 
	p != buddy_coll_fo.end(); p++){
	ldout(cct, 10) << __func__ <<  " cid = " << p->first << dendl;
      }
  }

  // collection info 
  BuddyCollFileObject* buddy_create_file_object (const coll_t& c);


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
//    CephContext* cct;
    bufferlist log_bl; // metadata log and checksum. flush in commit. 
    vector<buddy_buffer_head_t> bh_list; // to be flushed in commit 
//    vector<???> gb_list; // garbage 

    int log_entry_cnt;

    // log op entry 
    int add_log_entry(const coll_t& cid, const ghobject_t& oid, __le32 type){
      //ldout(cct, 10) << __func__ << " cid = " << cid << " oid = " << oid << " type = " << type << dendl;
      buddy_log_entry_t* ble = new buddy_log_entry_t(cid, oid, type);
      ble->encode(log_bl);
      log_entry_cnt++;
      return log_entry_cnt;
    }

    // log op entry 
    int add_log_entry(const coll_t& cid, const ghobject_t& oid, __le32 type, bufferlist &bl){
      //ldout(cct, 10) << __func__ << " cid = " << cid << " oid = " << oid << " type = " << type << " bl_length = " << bl.length() << dendl;
      buddy_log_entry_t* ble = new buddy_log_entry_t(cid, oid, type, bl);
      ble->encode(log_bl);
      log_entry_cnt++;
      return log_entry_cnt;
    }

    BuddyTransaction () {}
    ~BuddyTransaction () {}

  };

  ////////////////////////
  // Transactions' member functions? 

  vector<BuddyTransaction> running_tr;
  vector<BuddyTransaction> commit_tr;
  vector<BuddyTransaction> complete_tr;


  BuddyCollFileObject* get_coll_file_object(const coll_t& cid)
  {
    ldout(cct, 10) << __func__ << " cid = " << cid << dendl;
    map<coll_t, BuddyCollFileObject>::iterator p = buddy_coll_fo.find(cid);

    //assert(p != buddy_coll_fo.end()); // 있어야 함. 
    if (p == buddy_coll_fo.end()) {
      ldout(cct, 10) << "Failed to search. go through coll_map" << dendl;
      for (p = buddy_coll_fo.begin(); p != buddy_coll_fo.end(); p++){
	ldout(cct, 10) << __func__ <<  " cid = " << p->first << dendl;
      }
    }
    // insert? 

    return &p->second;
  }


  /****************************
   * sync_thread
   ***************************/
  int buddy_do_checkpoint(); 

#if 0
  struct BuddySyncThread : public Thread {
      BuddyFileStore *bfs;
        
      explicit SyncThread(BuddyFileStore *f) : fs(f) {}
          
      void *entry() override {
	fs->buddy_do_checkpoint();
	return 0;
      }
  } buddy_sync_thread;
#endif

  // 생성될때 초기화시키고 mount 에서 .create 로 시작시키기. 
  //////////////

  int buddy_mkfs();

  int buddy_create_collection(const coll_t& cid, uint32_t bits, BuddyTransaction& bt);
  int buddy_touch(const coll_t& cid, const ghobject_t& oid, BuddyTransaction& bt);
  int buddy_write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, 
      const bufferlist& bl, BuddyTransaction& bt, uint32_t fadvise_flags = 0); // bt
  int buddy_zero(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, BuddyTransaction& bt){ /* not implemented */ return 0;}
  int buddy_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size, BuddyTransaction& bt);
  int buddy_remove(const coll_t& cid, const ghobject_t& oid, BuddyTransaction& bt);
  int buddy_setattrs(const coll_t& cid, const ghobject_t& oid, map<string,bufferptr>& aset, BuddyTransaction& bt);
  int buddy_rmattr(const coll_t& cid, const ghobject_t& oid, const char *name, BuddyTransaction& bt);
  int buddy_rmattrs(const coll_t& cid, const ghobject_t& oid, BuddyTransaction& bt);
  int buddy_clone(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid, BuddyTransaction& bt);
  int buddy_clone_range(const coll_t& cid, const ghobject_t& oldoid,
		   const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff, BuddyTransaction& bt);
  int buddy_omap_clear(const coll_t& cid, const ghobject_t &oid, BuddyTransaction& bt);
  int buddy_omap_setkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& aset_bl, BuddyTransaction& bt);
  int buddy_omap_rmkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& keys_bl, BuddyTransaction& bt);
  int buddy_omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
		       const string& first, const string& last, BuddyTransaction& bt);
  int buddy_omap_setheader(const coll_t& cid, const ghobject_t &oid, const bufferlist &bl, BuddyTransaction& bt);

  //int buddy_collection_hint_expected_num_objs(const coll_t& cid, uint32_t pg_num,
  //    uint64_t num_objs) const { return 0; }
  int buddy_create_collection(const coll_t& c, int bits, BuddyTransaction& bt);
  int buddy_destroy_collection(const coll_t& c, BuddyTransaction& bt);
  int buddy_collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid, BuddyTransaction& bt);
  int buddy_collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
			      coll_t cid, const ghobject_t& o, BuddyTransaction& bt);
  int buddy_split_collection(const coll_t& cid, uint32_t bits, uint32_t rem, coll_t dest, BuddyTransaction& bt);

  // Transaction management 
  BuddyTransaction* buddy_get_transaction();
  int buddy_prepare_commit(BuddyTransaction& bt);
  int buddy_do_commit(BuddyTransaction& bt);
  int buddy_end_commit(BuddyTransaction& bt);

  int buddy_load();

  // 
  int mount();
  int umount();

  // constructor and destructor 
  BuddyFileStore (CephContext* cct, const string& basedir_) 
    : cct(cct),
      basedir(basedir_),
      buddy_sb(basedir, "buddy_sb"),
      buddy_meta_lfo(cct, basedir, "buddy_meta_log"),
      buddy_cmt_lfo(cct, basedir, "buddy_cmt_log")
    {}

  BuddyFileStore () {}
  ~BuddyFileStore () {}

};

#endif
