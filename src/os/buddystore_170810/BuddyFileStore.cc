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

#include "BuddyFileStore.h"


/********************************
 *  mkfs 
 ********************************/

int BuddyFileStore::buddy_mkfs()
{

  ldout(cct, 10) << "coll_t : "<< sizeof(coll_t) << "ghobject_t : " << sizeof(ghobject_t) << dendl; 
  ldout(cct, 10) <<  __func__ << " create map and csum files " << dendl;

  string fn;
  int flags = O_RDWR | O_CREAT;
  int fd;

#if 0
  // create a map file 
  fn = basedir + "/buddy_map";
  fd = ::open(fn.c_str(), flags, 0644); 
  if (fd < 0) {
    ldout(cct, 10) << __func__ << " failed to create map file" << dendl;
    return fd;
  }
  ::close(fd);

  // create a checksum file 
  fn = basedir + "/buddy_csum";
  fd = ::open(fn.c_str(), flags, 0644);
  if (fd < 0) {
     ldout(cct, 10) << __func__ << " failed to create csum file" << dendl;
    return fd;
  }
  ::close(fd);
#endif

  // create a buddy_superblock file 
  fn = basedir + "/buddy_superblock";
  fd = ::open(fn.c_str(), flags, 0644);
  if (fd < 0) {
     ldout(cct, 10) << __func__ << " failed to create csum file" << dendl;
    return fd;
  }
  ::close(fd);

  // create and open log files 
  buddy_meta_log.open();
  buddy_commit_log.open();

  return fd;
}

/********************************
 * transaction operations 
 ********************************/


/*****************
 * transaction creation 
 **********/
BuddyFileStore::BuddyTransaction* BuddyFileStore::buddy_get_transaction()
{
  ldout(cct,10) << __func__ << dendl;
  BuddyFileStore::BuddyTransaction* bt = new BuddyTransaction(cct, buddy_curr_version);
  buddy_curr_version++;
  //BuddyFileStore::BuddyTransaction* bt = new BuddyTransaction(cct, buddy_curr_version.inc());
  //running_tr.push_back(*bt);

  ldout(cct,10) << __func__ << " version " << bt->version << dendl;
  //ldout(cct,10) << __func__ << " version " << bt->version.read() << dendl;
  return bt;
}


/*********************
 *    touch 
 * *******************/
int BuddyFileStore::buddy_touch(const coll_t& cid, const ghobject_t& oid, 
    BuddyFileStore::BuddyTransaction& bt)
{

  ldout(cct,10) << __func__ << " oid " << oid << " cid " << cid << dendl;

  // find BuddyCollection 
  map<coll_t, BuddyCollection>::iterator p = buddy_coll_map.find(cid);
  assert(p != buddy_coll_map.end());

  BuddyCollection& bcoll = p->second;
  bcoll.create_object(oid);

  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_TOUCH);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;

  return 0;
}

/*********************
 *     write 
 * *******************/
int BuddyFileStore::buddy_write(const coll_t& cid, const ghobject_t& oid, uint64_t ooff, size_t bytes, const bufferlist& bl, 
    BuddyFileStore::BuddyTransaction& bt,uint32_t fadvise_flags)
{

  ldout(cct,10) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << " bytes " << bytes << dendl;


  // 0. find FileObject 
  BuddyCollection* bcoll = get_buddy_coll(cid);

  if(!bcoll) {
   ldout(cct, 10) << " Collection is not found " << dendl;
   return -1;
  }

  // 1. alloc_space  
  vector<buddy_iov_t> iov; 

  int r = bcoll->cdata_file.alloc_space(BD_DATA_T, oid, ooff, bytes, iov); 
  if (r < 0) 
    return -1; // failed to alloc space 
  if (r > 0) 
    bcoll->create_object(oid); 


  // 2. split data into multiple iovector  
  for(vector<buddy_iov_t>::iterator ip = iov.begin();
      ip != iov.end(); ++ip){

    bufferlist newdata;
    newdata.substr_of(bl, (*ip).ooff, (*ip).ooff + (*ip).bytes);
    (*ip).ooff = 0; // after bl split  

    // test : omit this bit later! 
    bufferlist tmp;
    ::encode(newdata, tmp); // Do not encode for data 

//    ldout(cct, 10) << "newdata len = " << newdata.length() << " encodedata len = " << tmp.length();

    // datalist swap
    (*ip).data_bl.claim(newdata);
    //(*ip).data_bl.swap(newdata);
    assert((*ip).bytes == (*ip).data_bl.length());
  }

  // 3. add io vector  list 
  bt.iov_list.insert(bt.iov_list.end(), iov.begin(), iov.end());

  // 4. add log_item.
  // arg list
  bufferlist arg_bl;
  ENCODE_START(1, 1, arg_bl);
  ::encode(ooff, arg_bl);
  ::encode(bytes, arg_bl);
  ::encode(iov, arg_bl);
  ENCODE_FINISH(arg_bl);

  r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_WRITE, arg_bl);

  ldout(cct, 10) << __func__ << " log entry = " << r << dendl;

  return 0;

}


/*********************
 *   truncate
 * *******************/
int BuddyFileStore::buddy_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size, 
    BuddyFileStore::BuddyTransaction& bt)
{
  ldout(cct,10) << __func__ << dendl;
 
  BuddyCollection* bcoll = get_buddy_coll(cid);

  if(!bcoll){
   ldout(cct,10) << "Failed to find coll" << dendl; 
   return -1;
  }
  bcoll->cdata_file.truncate_space(oid, size);

  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_TRUNCATE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;
}

/*********************
 *  remove 
 * *******************/
int BuddyFileStore::buddy_remove(const coll_t& cid, const ghobject_t& oid, 
    BuddyFileStore::BuddyTransaction& bt)
{

  ldout(cct,10) << __func__ << dendl;

  BuddyCollection* bcoll = get_buddy_coll(cid);

  if(!bcoll){
   ldout(cct,10) << "Failed to find coll" << dendl; 
   return -1;
  }
  int r = bcoll->delete_object(oid);
  if (r > 0) 
    ldout(cct,10) << oid << " is deleted" << dendl;
  else
    ldout(cct,10) << oid << " does not exist" << dendl;

  // log op entry 
  r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_REMOVE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}

/*********************
 *  setattrs 
 * *******************/
int BuddyFileStore::buddy_setattrs(const coll_t& cid, const ghobject_t& oid, 
    map<string,bufferptr>& aset, BuddyFileStore::BuddyTransaction& bt)
{
  
  // 사실 이거는.. off, len 으로 저장하는게 아니라.. 
  // string, value 로 저장하는 거라서 in-memory 구조를 유지해야하고. 
  // storage 에 저장할때는 read-modify-write 를 해야함. 
  // 그래서 이건 log 에실제 argument 까지 다 저장을 하고 
  // in-memory 구조만 업데이트 한 후에 checkpoint 때 전체를 플러시 해줘야 할듯. 
  // checkpoint 때에 내려줘야하는 애로 어딘가에 달아놔야함. 
  // in-memory 구조는 buddystore 에 있는걸로 해야함. 
  // 이 부분은. 어느정도 정리되면.. buddystore 랑 buddyfilestore 랑 코드를 합쳐야 할듯. 
  
  // add dirty attrs 
  bt.dirty_xattr_list.push_back(oid);

  // arg list 
  bufferlist arg_bl;
  ENCODE_START(1, 1, arg_bl);
  ::encode(aset, arg_bl);
  ENCODE_FINISH(arg_bl);

  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_SETATTRS, arg_bl);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}


/*********************
 *  rmattrs 
 * *******************/

int BuddyFileStore::buddy_rmattr(const coll_t& cid, const ghobject_t& oid, const char *name, 
    BuddyFileStore::BuddyTransaction& bt)
{

  // add dirty attrs 
  bt.dirty_xattr_list.push_back(oid);

  // arg list 
  bufferlist arg_bl;
  ENCODE_START(1, 1, arg_bl);
  ::encode(name, arg_bl);
  ENCODE_FINISH(arg_bl);

  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_RMATTR);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}
int BuddyFileStore::buddy_rmattrs(const coll_t& cid, const ghobject_t& oid, 
    BuddyFileStore::BuddyTransaction& bt)
{

  // add dirty attrs 
  bt.dirty_xattr_list.push_back(oid);

  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_RMATTRS);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}

/*********************
*  clone 
*********************/
 
int BuddyFileStore::buddy_clone(const coll_t& cid, const ghobject_t& oldoid, 
    const ghobject_t& newoid, BuddyFileStore::BuddyTransaction& bt)
{


  ldout(cct,10) << __func__ << dendl;
 
  BuddyCollection* bcoll = get_buddy_coll(cid);

  if(!bcoll){
   ldout(cct,10) << "Failed to find coll" << dendl; 
   return -1;
  }

  // 1. clone space
  // read data from src and return iov with bufferlist  
  vector<buddy_iov_t> iov;
  bcoll->cdata_file.clone_space(oldoid, newoid, iov);

  //add vector 
  bt.iov_list.insert(bt.iov_list.end(), iov.begin(), iov.end());

  // arg list
  bufferlist arg_bl;
  ENCODE_START(1, 1, arg_bl);
  ::encode(oldoid, arg_bl);
  ::encode(newoid, arg_bl);
  ::encode(iov, arg_bl);
  ENCODE_FINISH(arg_bl);

  // log op entry 
  int r = bt.add_log_item(cid, ghobject_t(), ObjectStore::Transaction::OP_CLONE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;
}


/*********************
*  clone range
*********************/

int BuddyFileStore::buddy_clone_range(const coll_t& cid, const ghobject_t& oldoid,
    const ghobject_t& newoid, uint64_t srcoff, uint64_t len, uint64_t dstoff, 
    BuddyFileStore::BuddyTransaction& bt)
{ 

  ldout(cct,10) << __func__ << dendl;
 
  BuddyCollection* bcoll = get_buddy_coll(cid);

  if(!bcoll){
   ldout(cct,10) << "Failed to find coll" << dendl; 
   return -1;
  }

  // 1. data clone 
  // read data from src and return iov with bufferlist  
  vector<buddy_iov_t> iov;
  bcoll->cdata_file.clone_space(oldoid, newoid, srcoff, len, dstoff, iov);

  //add vector 
  bt.iov_list.insert(bt.iov_list.end(), iov.begin(), iov.end());

  // 2. xattr, omap_header, omap clone 
  // to be implemented.. 
  // dirty list..  

  // arg list
  bufferlist arg_bl;
  ENCODE_START(1, 1, arg_bl);
  ::encode(oldoid, arg_bl);
  ::encode(newoid, arg_bl);
  ::encode(srcoff, arg_bl);
  ::encode(len, arg_bl);
  ::encode(dstoff, arg_bl);
  ENCODE_FINISH(arg_bl);

  // log op entry 
  int r = bt.add_log_item(cid, ghobject_t(), ObjectStore::Transaction::OP_CLONERANGE, arg_bl);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}
  

/*********************
*  omap_clear 
*********************/

int BuddyFileStore::buddy_omap_clear(const coll_t& cid, const ghobject_t &oid, BuddyFileStore::BuddyTransaction& bt)
{
  // add dirty omap info 

  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_OMAP_CLEAR);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;
}
  

int BuddyFileStore::buddy_omap_setkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& aset_bl, BuddyFileStore::BuddyTransaction& bt)
{

  // add dirty omap info 
 
 // arg list
  bufferlist arg_bl;
  ENCODE_START(1, 1, arg_bl);
  ::encode(aset_bl, arg_bl);
  ENCODE_FINISH(arg_bl);


  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_OMAP_SETKEYS);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}

int BuddyFileStore::buddy_omap_rmkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& keys_bl, BuddyFileStore::BuddyTransaction& bt)
{
  // log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_OMAP_RMKEYS);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}

  

int BuddyFileStore::buddy_omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
		       const string& first, const string& last, BuddyFileStore::BuddyTransaction& bt)
{ 
// log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_OMAP_RMKEYRANGE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}
  

int BuddyFileStore::buddy_omap_setheader(const coll_t& cid, const ghobject_t &oid, 
    const bufferlist &bl, BuddyFileStore::BuddyTransaction& bt)
{
// log op entry 
  int r = bt.add_log_item(cid, oid, ObjectStore::Transaction::OP_OMAP_SETHEADER);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;
}



/*********************
 *  create_collection  
 * *******************/

int BuddyFileStore::buddy_create_collection(const coll_t& cid, uint32_t bits, 
    BuddyFileStore::BuddyTransaction& bt)
{
  ldout(cct, 10) << __func__ << " cid = " << cid << " bits = " << bits << dendl;

  // find 
  map<coll_t, BuddyCollection>::iterator p = buddy_coll_map.find(cid);

  if (p != buddy_coll_map.end()){ // found  
    ldout(cct, 10) << "collection exists: " << cid << dendl;
    return 1;
  }

  // create BuddyCollection
  BuddyCollection* ncoll = new BuddyCollection(cct, basedir, cid);
  auto result = buddy_coll_map.insert(make_pair(cid, *ncoll));
  assert(result.second);
  ldout(cct, 10) << __func__ << " inserted cid " << cid << dendl;

  // create collection file 
  ldout(cct, 10) << __func__ << "coll file created: " << ncoll->cdata_file.fname << dendl;


  // log op entry 
  int r = bt.add_log_item(cid, ghobject_t(), ObjectStore::Transaction::OP_MKCOLL);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;

  return 0;
}


/*********************
 *  destroy_collection  
 * *******************/

int BuddyFileStore::buddy_destroy_collection(const coll_t& cid, BuddyFileStore::BuddyTransaction& bt)
{

  BuddyCollection* bcoll = get_buddy_coll(cid); 

  if(!bcoll) {
   ldout(cct, 10) << " Collection is not found " << dendl;
   return -1;
  }

  // erase collection from map 
  int dn = buddy_coll_map.erase(cid);
  ldout(cct, 10) << dn << " files have been deleted" << dendl;

  // delete collection 
  delete bcoll;

  // log op entry 
  int r = bt.add_log_item(cid, ghobject_t(), ObjectStore::Transaction::OP_RMCOLL);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

} 

int BuddyFileStore::buddy_collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid, BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_item(cid, ghobject_t(), ObjectStore::Transaction::OP_COLL_ADD);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;


}  
int BuddyFileStore::buddy_collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid, coll_t cid, const ghobject_t& o, BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_item(cid, ghobject_t(), ObjectStore::Transaction::OP_COLL_MOVE_RENAME);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}


int BuddyFileStore::buddy_split_collection(const coll_t& cid, uint32_t bits, uint32_t rem, 
    coll_t dest, BuddyFileStore::BuddyTransaction& bt)
{
// log op entry 
  int r = bt.add_log_item(cid, ghobject_t(), ObjectStore::Transaction::OP_SPLIT_COLLECTION);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;
}

// 
int BuddyFileStore::buddy_prepare_commit(BuddyTransaction& bt)
{

  // sync 필요하면 sync 불러줌. 예를 들면 mkcoll 같은거. 
  return 0;
}



/*********************************
 *    buddy_do_commit  
 *    This function flushes transaction data and op log 
 *    to stoarge.
 ********************************/
int BuddyFileStore::buddy_do_commit(BuddyTransaction& bt)
{

  ldout(cct, 10) << __func__ << dendl; 

  /***********
   * flush data 
   ***********/
  for (vector<buddy_iov_t>::iterator iovp = bt.iov_list.begin();
      iovp != bt.iov_list.end(); ++iovp)
  {
    buddy_iov_t& iov = *iovp;

    ldout(cct, 10) << "iov = " << iov << dendl;
    assert(iov.bytes == iov.data_bl.length());

    int r = iov.data_bl.write_file(iov.fname.c_str(), iov.foff, 0644);
    if (r) { // err
      ldout(cct, 10) << "Error in write "<< dendl;
      return -1;
    }

#if 0
    // verify 
    bufferlist test_bl;
    size_t len = bh.data_bl.length();
    string err;
    r = test_bl.read_file(bh.fname.c_str(), bh.foff, len, &err);
    if (r) {
      ldout(cct, 10) << "Error in read "<< dendl;
      return -1;
    }

    // bufferlist operator == is defined in buffer.h # 917
    if(!(test_bl == bh.data_bl)){
      ldout(cct, 10) << "Failed to verify write"<< dendl;
    }
#endif
  }
  /************
   * flush op 
   ***********/

  uint64_t lbytes, lalloc_bytes;
  off_t lfoff;
  lbytes = bt.log_bl.length();

  int r = buddy_commit_log.alloc_log_space(lbytes, lfoff, lalloc_bytes );
   
  if (r < 0) {
    ldout(cct, 10) << "Failed to alloc meta space" << dendl;
    return r;
  }

  buddy_commit_log.write_bl(lfoff, bt.log_bl);

#ifdef EUNJI_TEST 
  buddy_log_item_t* ble = new buddy_log_item_t();

  bufferlist::iterator bp = bt.log_bl.begin();
  for (int i = 0; i < bt.log_item_cnt; i++){
    ble->decode(bp);
    ldout(cct, 10) << "transaction op : cid = " << ble->cid << "op_type = " << ble->type << dendl;
  }
#endif
  //buddy_commit_log.alloc_space();

  delete &bt;

  return 0;
}

// 
int BuddyFileStore::buddy_end_commit(BuddyTransaction& bt)
{

  // 여기서는 private data 접근 가능한가?
  // remove removed files 
  //
  return 0;
}

/**********************************
 *  BuddyHashIndexFile functions 
 **********************************/


/******
 * create_file  
 */
int BuddyFileStore::BuddyHashIndexFile::create_file(int out_flags)
{
  ldout(cct,10) << __func__ << dendl;

  int flags = O_RDWR | O_CREAT;
  flags |= out_flags;

  int fd = ::open(fname.c_str(), flags, 0644);
  if (fd < 0){
    ldout(cct,10) << "Failed to create file: " << fname << dendl; 
    return fd;
  }

  // we can hold it for later .. 
  ::close(fd);
  fd = -1;

  return 0;
}

/******
 * delete_file  
 */
int BuddyFileStore::BuddyHashIndexFile::delete_file()
{
  ldout(cct,10) << __func__ << dendl;

  int r = ::unlink(fname.c_str());
  if (r < 0){
    ldout(cct,10) << "Failed to delete file: " << fname << dendl; 
  }
  return 0;
}




/******
 * alloc_space 
 */

int BuddyFileStore::BuddyHashIndexFile::alloc_space(int type, const ghobject_t& oid, 
    const off_t ooff, const ssize_t bytes, vector<buddy_iov_t>& iov)
{
  off_t hoff = hash_to_hoff(oid); 
  //off_t soff = hoff + ooff;
  ssize_t alloc_bytes = round_up(hoff + ooff + bytes) - hoff;
  off_t eoff = hoff + alloc_bytes; 
  off_t used_bytes = ooff + bytes;
  ssize_t limit_bytes = max_fbytes;

  // check pent hoff 
  map<off_t, buddy_hindex_t>::iterator p, prev, next;
  p = hash_index_map.lower_bound(hoff);
  prev = p;
  next = p;
  prev--;
  next++;

  // if (p == end?) // no bigger one
  // 	if (p == begin?) // no smaller one 
  //	   new_alloc
  // 	else
  // 	   lower_bound_check
  // 	   new_alloc
  //
  // if (it's me)
  // 	upper_bound_check
  // 	append or update
  //
  // else (found_hoff > hoff)
  // 	lower_bound_check
  // 	upper_bound_check
  // 	new_alloc
  // 	
  //
  if (p == hash_index_map.end()) {
    if (p != hash_index_map.begin()) {
      // lower_bound_check
      if (prev->second.get_alloc_end() >= hoff)
	return -1;
	//goto collision;
    }
    // upper limit 
    if(!(eoff < limit_bytes))
      return -1;
	//goto collision;

    // new_alloc 
    buddy_hindex_t* nhindex = new buddy_hindex_t(oid, BD_DATA_T, hoff, bytes, round_up(bytes)); 
    auto result = hash_index_map.insert(make_pair(hoff, *nhindex));
    assert(result.second);
  } 
  else {
    // it's me
    if (p->first == hoff) {

      //upper_bound_check 
      if (next != hash_index_map.end())
	limit_bytes = ++p->second.hoff;

      if(!(eoff < limit_bytes))
	return -1;
	//goto collision;
 
      // update index info 
      p->second.alloc_bytes = alloc_bytes > p->second.alloc_bytes? alloc_bytes : p->second.alloc_bytes; 
      p->second.used_bytes = used_bytes > p->second.used_bytes? used_bytes : p->second.used_bytes;
    } 
    // found bigger one 
    else {
      if (p != hash_index_map.begin()) {
    	// lower_bound_check
      	if (prev->second.get_alloc_end() >= hoff)
	  return -1;
  	  //goto collision;
      }

      //upper_bound_check 
      limit_bytes = p->first;
      
      if(!(eoff < limit_bytes))
	return -1;
	//goto collision;

      // new_alloc 
      buddy_hindex_t* nhindex = new buddy_hindex_t(oid, BD_DATA_T, hoff, bytes, round_up(bytes)); 
      auto result = hash_index_map.insert(make_pair(hoff, *nhindex));
      assert(result.second);
    }
  }

  // generate_iov
  buddy_iov_t* niov = new buddy_iov_t(BD_ORIG_F, fname, ooff, hoff + ooff, bytes); 
  iov.push_back(*niov);

  return 0;
}

int BuddyFileStore::BuddyHashIndexFile::release_space(const ghobject_t& oid)
{
  off_t hoff = hash_to_hoff(oid);
  return hash_index_map.erase(hoff);
}



/******
 * truncate_space 
 */
int BuddyFileStore::BuddyHashIndexFile::truncate_space(const ghobject_t& oid, ssize_t size)
{
  ldout(cct,10) << __func__  << " oid " << oid << " size " << size << dendl;

  off_t hoff = hash_to_hoff(oid);

  map<off_t, buddy_hindex_t>::iterator p = hash_index_map.find(hoff);

  if(p == hash_index_map.end())
    return -1; // no exist

  ldout(cct,10) << "before: oid " << oid << " ubytes " << p->second.used_bytes 
    << " abytes " << p->second.alloc_bytes << dendl;

  // add more space?
  ssize_t pad_bytes;
  off_t pad_off;
  if(size > p->second.used_bytes){
    pad_bytes = size - p->second.used_bytes;
    pad_off = p->second.used_bytes + 1;
    vector<buddy_iov_t> iov; 
    alloc_space(BD_DATA_T, oid, pad_off, pad_bytes, iov); 
  }
  else {
    p->second.used_bytes = size;
    p->second.alloc_bytes = round_up(p->second.used_bytes);
  }

  ldout(cct,10) << "after: oid " << oid << " ubytes " << p->second.used_bytes 
    << " abytes " << p->second.alloc_bytes << dendl;

  return 0;
}

/******
 * clone_space 
 */

int BuddyFileStore::BuddyHashIndexFile::clone_space(const ghobject_t& ooid, 
    const ghobject_t& noid, vector<buddy_iov_t>& iov) 
{
  ldout(cct,10) << __func__  << " ooid " << ooid << " noid " << noid << dendl;

  // get ooid space 
  off_t ohoff = hash_to_hoff(ooid);

  map<off_t, buddy_hindex_t>::iterator op = hash_index_map.find(ohoff);
  if (op == hash_index_map.end()){
    ldout(cct,10) << "old oid is not found" << dendl;
    return -1;
  }
  return clone_space(ooid, noid, 0, op->second.used_bytes, 0, iov);
}

int BuddyFileStore::BuddyHashIndexFile::clone_space(const ghobject_t& ooid, 
    const ghobject_t& noid, off_t srcoff, size_t bytes, off_t dstoff, 
    vector<buddy_iov_t>& iov)
{
  ldout(cct,10) << __func__  << " ooid " << ooid << " noid " << noid 
    << " srcoff " << srcoff << " bytes " << bytes << " dstoff " << dstoff<< dendl;

  // get ooid space 
  off_t ohoff = hash_to_hoff(ooid);
  off_t nhoff = hash_to_hoff(noid);
  ldout(cct,10) << "ohoff " << ohoff << " nhoff " << nhoff << dendl;

  map<off_t, buddy_hindex_t>::iterator op = hash_index_map.find(ohoff);
  if (op == hash_index_map.end()){
    ldout(cct,10) << "old oid is not found" << dendl;
    return -1;
  }

  // alloc space for new object 
  alloc_space(op->second.ctype, noid, 0, (ssize_t)(dstoff + bytes), iov);  
  
  ldout(cct,10) << "read_file " << fname << " " << ohoff+srcoff << "~" << bytes << dendl; 
  // read and copy 
  bufferlist src_bl;
  string err;
  src_bl.read_file(fname.c_str(), ohoff + srcoff, bytes, &err);

  ldout(cct,10) << "read size " << src_bl.length() << dendl;

  // write 
  // generate_iov()
  // buddy_iov_t* niov = new buddy_iov_t(BD_ORIG_F, fname, 0, nhoff + dstoff, bytes);
  // alloc_space create iovector in iov 
  for(vector<buddy_iov_t>::iterator ip = iov.begin();
      ip != iov.end(); ip++){
    assert(ip->data_bl.length() == 0);
    ip->data_bl.substr_of(src_bl, (*ip).ooff, (*ip).ooff + (*ip).bytes);
    (*ip).ooff = 0;
  }
  //niov->data_bl.claim(src_bl);

  return 0;

}


/**************************
 * BuddyLogFileObject
 **************************/

int BuddyFileStore::BuddyLogFileObject::alloc_log_space ( 
    const uint64_t bytes, off_t& foff, uint64_t& alloc_bytes)
    //const ssize_t bytes, uint64_t& foff, ssize_t& alloc_bytes)
{

  ldout(cct, 10) << __func__ << " requested_bytes = " << bytes << dendl; 
  // need to fix here later.. 
  // remaining bytes < bytes .. 
  // back to head ... 
  // locking ... 
  
  ldout(cct, 10) << __func__ << " before allocation: tail_pos = " << tail_pos << dendl;
  foff = tail_pos;
  alloc_bytes = ((bytes >> BUDDY_FILE_BSIZE_BITS) + 1) << BUDDY_FILE_BSIZE_BITS;
  tail_pos += alloc_bytes;
  assert (!(tail_pos & ((1 << BUDDY_FILE_BSIZE_BITS)-1)));
  ldout(cct, 10) << __func__ << " after allocation: tail_pos = " << tail_pos << dendl;

  ldout(cct, 10) << __func__ << " foff = " << foff << " alloc_bytes = " << alloc_bytes << dendl; 

  return 0;
}

#if 0
int BuddyFileStore::BuddyLogFileObject::write_bl(off64_t& pos, bufferlist& bl)
{
  int ret;

  off64_t spos = ::lseek64(fd, pos, SEEK_SET);
  if (spos < 0) {
    ret = -errno;
    derr << "FileJournal::write_bl : lseek64 failed " << cpp_strerror(ret) << dendl;
    return ret;
  }
  ret = bl.write_fd(fd);
  if (ret) {
    derr << "FileJournal::write_bl : write_fd failed: " << cpp_strerror(ret) << dendl;
    return ret;
  }
  pos += bl.length();
  if (pos == header.max_size)
    pos = get_top();
  return 0;
}
#endif


/*************************************
 *  do_checkpoint() : flush all map 
 *  checkpoint buddystore 
 *  dirty xattr, omap_header, omap
 *************************************/

int BuddyFileStore::buddy_do_checkpoint()
{

  // sync metadata: object_map, coll_map, superblock  
  ldout(cct, 10) << __func__ << dendl;

  size_t bytes = 0;
  uint64_t alloc_bytes; 
  off_t foff;
  size_t pos = 0;

#if 0
  // flush dirty xattr, omap_header, omap
  ghobject_t& xoid; 

  while(!dirty_xattr_list.empty()){
    xoid = dirty_xattr_list.front();
    ldout(cct,10) << " xattr update oid " << xoid << dendl;
    dirty_xattr_list.pop_front();
  }
#endif
  // flush system-wide metadata : 
  // 1. collection 
  // 2. object map 
  //
  // This information is encoded in a meta bufferlist. 
  // Each collections's data is followed by collection map info.  
  // After all superblock is written to superblock file 
  // with offset information for metadata section. 
  //
  // Consistency is enforced by checksum in a superblock. 
  // No enforced ordering.  


  // 1. collection  
  // collection : collection meta pos 
  //
  map<coll_t, uint64_t> collections; 
  bufferlist meta_bl; // collection 각각의 object map 을 encode 해서 들고 있을 bufferlist 

  for (map<coll_t, BuddyCollection>::iterator p = buddy_coll_map.begin();
     p != buddy_coll_map.end(); p++)
  { 

    // 2. object map of each collection 
    p->second.encode(meta_bl);

    ldout(cct, 10) << " cid : obj_num = " << p->first << " : " << 
      p->second.buddy_object_map.size() << dendl;

    // 1. collection: construct data structure for on-storage collection info 
    auto result = collections.insert(make_pair(p->first, pos));
    if(!result.second){
      ldout(cct, 10) << "collection insert failed" << dendl;
    }

    bytes = meta_bl.length() - bytes;
    pos += bytes; 
  }

  // 1. collection map 
  ::encode(collections, meta_bl);
  bytes = meta_bl.length() - bytes;
  ldout(cct, 10) << "collections location: pos = " << pos << " len = " << bytes << dendl;


  // alloc log space 
  int r = buddy_meta_log.alloc_log_space(bytes, foff, alloc_bytes);

  if (r < 0) {
    ldout(cct, 10) << "Failed to alloc meta space" << dendl;
    return r;
  }
 
  // flush metadata information 
  ldout(cct, 10) << "omap allocated at " << foff << " length " << alloc_bytes << dendl;
  //r = meta_bl.write_file(buddy_meta_log.fname.c_str(), foff);
  buddy_meta_log.write_bl(foff, meta_bl);

  if (r < 0) {
    ldout(cct, 10) << "Error in write_metafile with " << r << dendl;
    return r;
  }

  // flush superblock 
  buddy_superblock.last_cp_off = foff; // collections' position 
  buddy_superblock.last_cp_len = alloc_bytes;
  buddy_superblock.last_cp_coll_off = foff + pos; 
  buddy_superblock.last_cp_coll_len = bytes;
  //uint64_t v = buddy_curr_version.inc();
  //buddy_superblock.last_cp_version.set(buddy_curr_version); 
  buddy_superblock.last_cp_version = buddy_curr_version; 
  buddy_superblock.last_cp_version++; 

  r = write_buddy_superblock();

  if (r < 0) {
    ldout(cct, 10) << "Error in write_buddy_superblock with " << r << dendl;
    return r;
  }

  return 0;
}


/********************************
 * superblock operations 
 ********************************/
int BuddyFileStore::write_buddy_superblock()
{
  ldout(cct,10) << __func__ << buddy_superblock << dendl; 
  bufferlist bl;
//  ::encode(buddy_superblock, bl);
  buddy_superblock.encode(bl);
  assert(bl.length() < (1 << BUDDY_FILE_BSIZE_BITS)); 
  ldout(cct,10) << __func__ << " bl.length: " << bl.length() << dendl; 

  string fn = buddy_superblock.fname;
  int r = bl.write_file(fn.c_str());

  ldout(cct,10) << __func__ << " wrote: " << r << dendl; 

  return r;
}

int BuddyFileStore::read_buddy_superblock()
{

  ldout(cct,10) << __func__ << dendl; 

#if 0
  bufferlist bl;
  string err;
  
  int r = bl.read_file(buddy_superblock.fname.c_str(), &err);
  if (r < 0) {
    ldout(cct, 10) << "Failed to read buddy_superblock : " << err << dendl;
    return r;
  }

  bufferlist::iterator i = bl.begin();
  buddy_superblock.decode(i);

  return 0;

#endif
  //bufferlist::iterator p;
  bufferptr bp(BUDDY_FILE_BSIZE); // < 4KB

  int r = safe_read_file(basedir.c_str(), "buddy_superblock", 
      bp.c_str(), bp.length());

  if (r < 0) {
    ldout(cct,10) << __func__ << " No superblock file " << dendl; 
    return r;
  }

  if (r == 0){
    ldout(cct,10) << __func__ << " Empty superblock file " << dendl; 
    return r;
  }

  ldout(cct,10) << __func__ << " read: " << r << dendl; 

  bufferlist bl;
  
  bl.push_back(std::move(bp));
  bufferlist::iterator i = bl.begin();

  buddy_superblock.decode(i);

  ldout(cct,10) << __func__ << " last_cp_off = " << buddy_superblock.last_cp_off
    << " last_cp_version = " << buddy_superblock.last_cp_version << dendl; 

   return 0;
}


/********************************
 *   buddy_load 
 ********************************/

int BuddyFileStore::buddy_load()
{

  ldout(cct,10) << __func__ << dendl; 

  int r;
  // 1. read superblock 
  r = read_buddy_superblock();
  if (r < 0) {
    ldout(cct, 10) << "Error in read_buddy_superblock" << dendl;
    return r;
  }
 
  // 1-1. set current version 
  //buddy_curr_version.set(buddy_superblock.last_cp_version); 
  buddy_curr_version = buddy_superblock.last_cp_version; 

 
  // 2. read collections
  ldout(cct, 10) << __func__ << "read collections " << dendl;
  bufferlist bl;

  r = buddy_meta_log.open(); // create or open 

  if (r < 0) {
    ldout(cct, 10) << "Failed to open buddy_meta_log" << dendl;
    return r;
  }

  // file read 
  r = buddy_meta_log.read_bl(
	buddy_superblock.last_cp_off,
	buddy_superblock.last_cp_len,
	bl);

  if (bl.length() == 0){
      ldout(cct, 10) << "Empty file" << dendl;
      return r;
  }

  // decode collections  
  ldout(cct, 10) << "read = " << bl.length() << dendl;
  
  bufferlist coll_bl;
  coll_bl.substr_of(bl, 
      buddy_superblock.last_cp_coll_off,
      buddy_superblock.last_cp_coll_off + buddy_superblock.last_cp_coll_len);

  map<coll_t, uint64_t> collections;
  bufferlist::iterator p = coll_bl.begin();
  ::decode(collections, p);


  // 3. BuddyCollection and buddy_object_map
  ldout(cct, 10) << "Read object map " << dendl;

#if 0
  // 사실 불필요하게 두번 읽고 있지..
  r = fo_bl.read_file (
    buddy_meta_log.fname.c_str(), 
    buddy_superblock.last_cp_off,
    buddy_superblock.last_cp_len,
    &err
    );

  if (r < 0) {
    ldout(cct, 10) << "Error in read collections' data" << dendl;
    return r;
  }

  if (fo_bl.length() == 0){
    ldout(cct, 10) << "Empty file" << dendl;
    return r;
  }

  ldout(cct, 10) << "read = " << fo_bl.length() << dendl;
#endif


  bufferlist::iterator fp = bl.begin();

  // create BuddyCollection 
  for(map<coll_t, uint64_t>::iterator cp = collections.begin();
      cp != collections.end(); cp++)
  {

  // build BuddyCollection
    BuddyCollection* bcoll = new BuddyCollection(cct, basedir, cp->first);
    bcoll->decode(fp);


    // insert BuddyCollection
    auto result = buddy_coll_map.insert(make_pair(cp->first, (*bcoll)));
    assert(result.second);

    ldout(cct, 10) << (*cp) << " has " << bcoll->buddy_object_map.size() << " objects" << dendl;
  }


  // 4. check log file 
  r = buddy_commit_log.open();
  if (r < 0){ 
    ldout(cct, 10) << "Failed to open buddy_commit_log" << dendl;
    return r;
  }
  // header ... 
  
  return 0;

#if 0
  string err;
  r = bl.read_file (
      buddy_meta_log.fname.c_str(), 
      buddy_superblock.last_cp_coll_off,
      buddy_superblock.last_cp_coll_len,
      &err
      ); // read whole file 
  if (r < 0) {
    ldout(cct, 10) << "Error in read collections" << dendl;
    return r;
  }
#endif

}


/********************************
 * mount / umount 
 ********************************/


int BuddyFileStore::mount()
{
  buddy_load();
  return 0;
}

int BuddyFileStore::umount()
{
  buddy_do_checkpoint();

  buddy_meta_log.close();
  buddy_commit_log.close();
  return 0;
}
