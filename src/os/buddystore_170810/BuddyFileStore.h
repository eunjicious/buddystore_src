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
#include "include/atomic.h"

#include "common/debug.h"
#include "common/TrackedOp.h"
#include "include/Context.h"
#include "common/safe_io.h"
//#include "BitMap.h"
//#include "include/buddy_types.h"
#include "buddy_types.h"

#define BUDDY_FILE_BSIZE_BITS 12
#define BUDDY_FILE_BSIZE 1 << 12
//
//#define dout_context cct
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddyfilestore " 
//<< basedir << ") " 

class CephContext;

using std::vector;
using std::string;
using std::map;

namespace ceph {
  class Formatter;
}

///////////
class BuddyFileStore {
//  atomic_t buddy_curr_version;
  uint64_t buddy_curr_version;
public:
  CephContext* cct;
  string basedir;

  buddy_superblock_t buddy_superblock;
  int write_buddy_superblock();
  int read_buddy_superblock();

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
    uint64_t max_file_size;

    uint64_t head_pos;
    uint64_t tail_pos;
    uint64_t commit_pos; // dont need? 

  private:
    int fd;

  public:
    int open(){
      int flags = O_RDWR | O_CREAT;
      fd = ::open(fname.c_str(), flags, 0644);
      if (fd < 0) {
	ldout(cct,10) << "Failed to open " << fname << dendl;
      }
      return fd;
    }

    int close() {
      int r = ::close(fd);
      if (r != 0) {
	ldout(cct,10) << "Failed to close " << fname << dendl;
      }
      return r;
    }

    int write_bl(off_t foff, bufferlist& bl) {
      return bl.write_fd(fd, foff);
    }
    int read_bl(off_t foff, ssize_t len, bufferlist& bl){
      return bl.read_fd(fd, foff, len);
    }



  public:
    int alloc_log_space (const uint64_t len, off_t& foff, uint64_t& alloc_bytes);

    // constructor 
    explicit BuddyLogFileObject(CephContext* cct_, string basedir_, string fname_) : cct(cct_)
    {
      basedir = basedir_;
      fname = basedir + "/"+ fname_;  // 여기서부터 하기.
      max_file_size = std::numeric_limits<uint64_t>::max();
    }
    BuddyLogFileObject() {}
    ~BuddyLogFileObject(){}
    
  }; // class BuddyLogFileObject

  BuddyLogFileObject buddy_meta_log; // object_map, coll_map 
  BuddyLogFileObject buddy_commit_log; // checksum, op logging 

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
  class BuddyHashIndexFile {

  public:
    CephContext* cct;
    coll_t cid; 

    // file information 
    string fname;
    int type;
    int fd;

  private:
    // space management 
    uint64_t max_fbytes;
    uint64_t used_fbytes;

  public:
    //BitMap shadow_bitmap;
    //map<off_t, buddy_hindex_t> free_index_map;
    map<off_t, buddy_hindex_t> hash_index_map;

    int create_file(int flag);
    int delete_file();

    int alloc_space(int type, const ghobject_t& oid, const off_t ooff, const ssize_t bytes, 
      vector<buddy_iov_t>& iov);
    int release_space(const ghobject_t& oid);
    int truncate_space(const ghobject_t& oid, ssize_t size); 

    int clone_space(const ghobject_t& ooid, const ghobject_t& noid, vector<buddy_iov_t>& iov); 
    int clone_space(const ghobject_t& ooid, const ghobject_t& noid, off_t srcoff, size_t bytes, 
	off_t dstoff, vector<buddy_iov_t>& iov);

    // store HashIndexFile info persistently
    // - hash_index_map 
    // - shadow bitmap 
    // - max_fbytes, used_fbytes
    void encode(bufferlist& bl) const 
    {
      ENCODE_START(1, 1, bl);
      ::encode(hash_index_map, bl);
      ::encode(max_fbytes, bl);
      ::encode(used_fbytes, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p)
    {
      DECODE_START(1, p);
      ::decode(hash_index_map, p);
      ::decode(max_fbytes, p);
      ::decode(used_fbytes, p);
      DECODE_FINISH(p);
    }

    // constructor 
    explicit BuddyHashIndexFile(CephContext* cct_, const coll_t& cid_, string fname_): 
      cct(cct_), 
      cid(cid_),
      fname(fname_) {

      max_fbytes= std::numeric_limits<uint64_t>::max();
      used_fbytes = 0;

      create_file(0);
    }
    BuddyHashIndexFile() {}
    ~BuddyHashIndexFile(){
      delete_file();
    }
    
  private: 
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
      return ((v >> BUDDY_FILE_BSIZE_BITS)+1) << BUDDY_FILE_BSIZE_BITS;
    }

    off_t hash_to_hoff(const ghobject_t& oid){
      uint64_t bnum = (uint64_t)_reverse_bits(oid.hobj.get_hash());
      return bnum << BUDDY_FILE_BSIZE_BITS; // hash(blk num) to bytes 
    }

  }; // class BuddyHashIndexFile


  /********************************
   *  BuddyCollection 
   *******************************/

  class BuddyCollection {
  public:
    CephContext* cct;
    coll_t cid; // 저장해야 하지만. 이건 외부에서 저장한다고 치고.  

    string basedir;
    string basefname;

    set<ghobject_t> buddy_object_map;

    // collection data file 
    BuddyHashIndexFile cdata_file;
   
    // create and delete objects  
    int create_object(const ghobject_t& oid){
      auto result = buddy_object_map.insert(oid);
      if(!result.second)
	return -EEXIST;
      return 0;
    }

    int delete_object(const ghobject_t& oid){
      // release file space 
      int r = cdata_file.release_space(oid);
      ldout(cct,10) << r << " objects have been deleted" << dendl;
      return buddy_object_map.erase(oid);
    }

    // on-storage collection's info 
    // - buddy_object_map : object list in collection
    // - file info  
    void encode(bufferlist& bl) const 
    {
      ENCODE_START(1, 1, bl);
      ::encode(buddy_object_map, bl);
      cdata_file.encode(bl);
      ENCODE_FINISH(bl);

    }
    void decode(bufferlist::iterator& p)
    {
      DECODE_START(1, p);
      ::decode(buddy_object_map,p);
      cdata_file.decode(p);
      DECODE_FINISH(p);
    }


   explicit BuddyCollection(CephContext* cct_, string basedir_, const coll_t& cid_) : 
      cct(cct_), 
      cid(cid_),
      basedir(basedir_),
      basefname(basedir + "/" + "buddy_"+cid.to_str()),
      cdata_file(cct, cid, basefname + ".data") {}

    BuddyCollection(){}
    ~BuddyCollection(){}
  };


  /*********************** 
   * global collection map
   ***********************/
  map<coll_t, BuddyCollection> buddy_coll_map;
  
  void stat_buddy_coll_map()
  {
    ldout(cct, 10) << __func__ << " coll_map size = " << buddy_coll_map.size() << dendl;

    for (map<coll_t, BuddyCollection>::iterator p = buddy_coll_map.begin(); 
	p != buddy_coll_map.end(); p++){
	ldout(cct, 10) << __func__ <<  " cid = " << p->first << dendl;
      }
  }


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
    CephContext* cct;
    uint64_t version;
    bufferlist log_bl; // metadata log and checksum. flush in commit. 
    vector<buddy_iov_t> iov_list; // to be flushed in commit 
//    vector<???> gb_list; // garbage 

    list<ghobject_t> dirty_xattr_list;
    list<ghobject_t> dirty_omaph_list;
    list<ghobject_t> dirty_omap_list;

    int log_item_cnt;

    // log op entry 
    int add_log_item(const coll_t& cid, const ghobject_t& oid, __le32 type){
      //ldout(cct, 10) << __func__ << " cid = " << cid << " oid = " << oid << " type = " << type << dendl;
      buddy_log_item_t* ble = new buddy_log_item_t(cid, oid, type);
      ble->encode(log_bl);
      log_item_cnt++;
      return log_item_cnt;
    }

    // log op entry 
    int add_log_item(const coll_t& cid, const ghobject_t& oid, __le32 type, bufferlist &bl){
      //ldout(cct, 10) << __func__ << " cid = " << cid << " oid = " << oid << " type = " << type << " bl_length = " << bl.length() << dendl;
      buddy_log_item_t* ble = new buddy_log_item_t(cid, oid, type, bl);
      ble->encode(log_bl);
      log_bl.claim_append(bl, buffer::list::CLAIM_ALLOW_NONSHAREABLE); 
      log_item_cnt++;
      return log_item_cnt;
    }


    explicit BuddyTransaction (CephContext* cct_, uint64_t v): cct(cct_) {
      version = v;
    }
    BuddyTransaction () {}
    ~BuddyTransaction () {}

  };

  ////////////////////////
  // Transactions' member functions? 

  vector<BuddyTransaction> running_tr;
  vector<BuddyTransaction> commit_tr;
  vector<BuddyTransaction> checkpoint_tr;


  BuddyCollection* get_buddy_coll(const coll_t& cid)
  {
    ldout(cct, 10) << __func__ << " cid = " << cid << dendl;
    map<coll_t, BuddyCollection>::iterator p = buddy_coll_map.find(cid);

    if (p == buddy_coll_map.end()) {
      ldout(cct, 10) << "Failed to search. go through coll_map" << dendl;
      for (p = buddy_coll_map.begin(); p != buddy_coll_map.end(); p++){
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
      buddy_superblock(basedir, "buddy_superblock"),
      buddy_meta_log(cct, basedir, "buddy_meta_log"),
      buddy_commit_log(cct, basedir, "buddy_cmt_log")
    {}

  BuddyFileStore () {}
  ~BuddyFileStore () {}

};

#endif
