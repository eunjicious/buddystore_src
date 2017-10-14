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

void buddy_index_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl); // 이 숫자는 뭔지 잘 모르겠음. 
  //ghobject_t 를 같이 저장할지 말지는 조금 더 고민. map 에 어차피 저장함.
  ::encode(oid, bl);
  ::encode(type, bl);
  ::encode(ooff, bl);
  ::encode(foff, bl);
  ::encode(used_bytes, bl);
  ::encode(alloc_bytes, bl);
  ENCODE_FINISH(bl);
}

void buddy_index_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(oid, p);
  ::decode(type, p);
  ::decode(ooff, p);
  ::decode(foff, p);
  ::decode(used_bytes, p);
  ::decode(alloc_bytes, p);
  DECODE_FINISH(p);
}
bool operator< (const buddy_index_t& b1, const buddy_index_t& b2)
{
  return b1.oid < b2.oid;
}

bool operator> (const buddy_index_t& b1, const buddy_index_t& b2)
{
  return b1.oid < b2.oid;
}

ostream& operator<< (ostream& out, const buddy_index_t& bi)
{
  out << bi.type << ':';
  out << bi.ooff << ':';
  out << bi.used_bytes << ':';
  out << bi.foff << ':';
  out << bi.alloc_bytes << ':';

  return out;
}

///// mkfs 
int BuddyFileStore::buddy_mkfs()
{

  ldout(cct, 10) << "coll_t : "<< sizeof(coll_t) << "ghobject_t : " << sizeof(ghobject_t) << dendl; 
  ldout(cct, 10) <<  __func__ << " create map and csum files " << dendl;

  string fn;
  int flags = O_RDWR | O_CREAT;
  int fd;

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
  close(fd);

  // create a buddy_sb file 
  fn = basedir + "/buddy_sb";
  fd = ::open(fn.c_str(), flags, 0644);
  if (fd < 0) {
     ldout(cct, 10) << __func__ << " failed to create csum file" << dendl;
    return fd;
  }
  close(fd);

  return fd;
}

BuddyFileStore::BuddyTransaction* BuddyFileStore::buddy_get_transaction()
{

  BuddyFileStore::BuddyTransaction* bt = new BuddyTransaction();
  running_tr.push_back(*bt);
  return bt;
}


/********************************
 * transaction operations 
 ********************************/

/*********************
 *    touch 
 * *******************/
int BuddyFileStore::buddy_touch(const coll_t& cid, const ghobject_t& oid, 
    BuddyFileStore::BuddyTransaction& bt)
{

  ldout(cct,10) << __func__ << " oid " << oid << dendl;

  // find BuddyCollFileObject of collection 
  map<coll_t, BuddyCollFileObject>::iterator p = buddy_coll_fo.find(cid);
  assert(p != buddy_coll_fo.end());

  BuddyCollFileObject& fo = p->second;

  // insert new object entry
  buddy_index_t* findex = new buddy_index_t(oid, none_t, 0, 0, 0, 0);
  fo.insert_object_index(oid, *findex);

  // insert object map
  fo.buddy_object_map.insert(*findex);

  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_TOUCH);
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

  uint64_t foff;
  size_t alloc_bytes; // new position 

  // 0. find FileObject 
  BuddyCollFileObject* fo = get_coll_file_object(cid);
  if(!fo) {
   ldout(cct, 10) << " File is not found " << dendl;
   return -1;
  }

  // 1. alloc_space : 여기에서 map_info 까지 업데이트 해줌.  
  int r = fo->alloc_space(data_t, oid, ooff, bytes, foff, alloc_bytes); 
  if (r < 0){
    return -1;
  }

  // 2. bh를 업데이트 하고 bt 의 data_bl 에 담기 
  buddy_buffer_head_t* bh = new buddy_buffer_head_t(fo->fname, foff, alloc_bytes); 
  //ENCODE_START(1, 1, bh->data_bl);
  ::encode(bl, bh->data_bl);
  //ENCODE_FINISH(bh->data_bl);
  bt.bh_list.push_back(*bh);

  // 3. log_entry 만들어 넣기.
  // arg list 만들기 

  bufferlist arg_bl;
  ENCODE_START(1, 1, arg_bl);
  ::encode(ooff, arg_bl);
  ::encode(bytes, arg_bl);
  ::encode(foff, arg_bl);
  ENCODE_FINISH(arg_bl);

  r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_WRITE, arg_bl);

  ldout(cct, 10) << __func__ << " log entry = " << r << dendl;

  return 0;

}



/*********************
 *   truncate
 * *******************/
int BuddyFileStore::buddy_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size, 
    BuddyFileStore::BuddyTransaction& bt)
{

  BuddyCollFileObject* fo = get_coll_file_object(cid);

  if(!fo) 
    return -1;

  // object map 에서 해당 object 의 index 를 찾아야 할듯. 
  // 마지막 노드를 찾으면 됨. 끝에서 작업하는 거니까. 
  // 즉, 거꾸로 검색하면서..  만약 늘리는 거면 alloc_space 로 추가할당 받으면 되고 
  // 줄이는 거면 extent 에서 bytes 조정하고 free space 로 반납하면 되겠군. 
  // 그리고 나서 줄이거나 늘려야 겠지 


  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_TRUNCATE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;
}

/*********************
 *  remove 
 * *******************/
int BuddyFileStore::buddy_remove(const coll_t& cid, const ghobject_t& oid, 
    BuddyFileStore::BuddyTransaction& bt)
{
  // relase_file_range()
  //

  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_REMOVE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}


int BuddyFileStore::buddy_setattrs(const coll_t& cid, const ghobject_t& oid, 
    map<string,bufferptr>& aset, BuddyFileStore::BuddyTransaction& bt)
{
  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_SETATTRS);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}


int BuddyFileStore::buddy_rmattr(const coll_t& cid, const ghobject_t& oid, const char *name, 
    BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_RMATTR);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;


}
int BuddyFileStore::buddy_rmattrs(const coll_t& cid, const ghobject_t& oid, 
    BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_RMATTRS);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}
  
int BuddyFileStore::buddy_clone(const coll_t& cid, const ghobject_t& oldoid, 
    const ghobject_t& newoid, BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_entry(cid, ghobject_t(), ObjectStore::Transaction::OP_CLONE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;


}


int BuddyFileStore::buddy_clone_range(const coll_t& cid, const ghobject_t& oldoid,
    const ghobject_t& newoid, uint64_t srcoff, uint64_t len, uint64_t dstoff, 
    BuddyFileStore::BuddyTransaction& bt)
{ 

  // log op entry 
  int r = bt.add_log_entry(cid, ghobject_t(), ObjectStore::Transaction::OP_CLONERANGE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}
  

int BuddyFileStore::buddy_omap_clear(const coll_t& cid, const ghobject_t &oid, BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_OMAP_CLEAR);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;


}
  

int BuddyFileStore::buddy_omap_setkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& aset_bl, BuddyFileStore::BuddyTransaction& bt)
{
  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_OMAP_SETKEYS);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}

int BuddyFileStore::buddy_omap_rmkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& keys_bl, BuddyFileStore::BuddyTransaction& bt)
{
  // log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_OMAP_RMKEYS);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}

  

int BuddyFileStore::buddy_omap_rmkeyrange(const coll_t& cid, const ghobject_t &oid,
		       const string& first, const string& last, BuddyFileStore::BuddyTransaction& bt)
{ 
// log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_OMAP_RMKEYRANGE);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}
  

int BuddyFileStore::buddy_omap_setheader(const coll_t& cid, const ghobject_t &oid, 
    const bufferlist &bl, BuddyFileStore::BuddyTransaction& bt)
{
// log op entry 
  int r = bt.add_log_entry(cid, oid, ObjectStore::Transaction::OP_OMAP_SETHEADER);
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
  map<coll_t, BuddyCollFileObject>::iterator p = buddy_coll_fo.find(cid);

  if (p != buddy_coll_fo.end()){ // found  
    ldout(cct, 10) << "collection exists: " << cid << dendl;
    return 1;
  }

  // create BuddyCollFileObject

  BuddyCollFileObject* bdfo = new BuddyCollFileObject(cct, basedir, cid);
  auto result = buddy_coll_fo.insert(make_pair(cid, *bdfo));
  assert(result.second);

  ldout(cct, 10) << __func__ << " inserted cid " << cid << dendl;
  // test 
  p = buddy_coll_fo.find(cid);
  assert(p != buddy_coll_fo.end());
  ldout(cct, 10) << __func__ << " found cid = " << cid << dendl;

  // create a collection file  
  //string fn = basedir + cid.to_str();
  string fn = bdfo->fname;

  int flags = O_RDWR;

  flags |= O_CREAT;

  int fd = ::open(fn.c_str(), flags, 0644);
  if (fd < 0) {

    ldout(cct, 10) << __func__ << "failed to create coll file " << fn << dendl;
    return -ENOENT;
  }

  VOID_TEMP_FAILURE_RETRY(::close(fd));


  // log op entry 
  int r = bt.add_log_entry(cid, ghobject_t(), ObjectStore::Transaction::OP_MKCOLL);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  ldout(cct, 10) << __func__ << "coll file created: " << fn << dendl;

  return 0;
}


/*********************
 *  destroy_collection  
 * *******************/

int BuddyFileStore::buddy_destroy_collection(const coll_t& cid, BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_entry(cid, ghobject_t(), ObjectStore::Transaction::OP_RMCOLL);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;


} 

int BuddyFileStore::buddy_collection_add(const coll_t& cid, const coll_t& ocid, const ghobject_t& oid, BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_entry(cid, ghobject_t(), ObjectStore::Transaction::OP_COLL_ADD);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;


}  
int BuddyFileStore::buddy_collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid, coll_t cid, const ghobject_t& o, BuddyFileStore::BuddyTransaction& bt)
{

  // log op entry 
  int r = bt.add_log_entry(cid, ghobject_t(), ObjectStore::Transaction::OP_COLL_MOVE_RENAME);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;

}


int BuddyFileStore::buddy_split_collection(const coll_t& cid, uint32_t bits, uint32_t rem, 
    coll_t dest, BuddyFileStore::BuddyTransaction& bt)
{
// log op entry 
  int r = bt.add_log_entry(cid, ghobject_t(), ObjectStore::Transaction::OP_SPLIT_COLLECTION);
  ldout(cct, 10) << __func__ << "log entry = " << r << dendl;
  return 0;
}


void BuddyFileStore::buddy_sb_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(last_cp_off, bl);
  ::encode(last_cp_len, bl);
  ::encode(last_cp_coll_off, bl);
  ::encode(last_cp_coll_len, bl);
  ::encode(last_cp_version, bl);
  ENCODE_FINISH(bl);
}


void BuddyFileStore::buddy_sb_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(last_cp_off, p);
  ::decode(last_cp_len, p);
  ::decode(last_cp_coll_off, p);
  ::decode(last_cp_coll_len, p);
  ::decode(last_cp_version, p);
  DECODE_FINISH(p);
}

ostream& operator<<(ostream& out, const BuddyFileStore::buddy_sb_t& o)
{
  out << " last_cp = ";
  out << o.last_cp_off << ':';
  out << o.last_cp_len;
  out << " last_cp_coll = ";
  out << o.last_cp_coll_off << ':'; 
  out << o.last_cp_coll_len;
  out << " last_cp_version = ";
  out << o.last_cp_version; 
				        
  return out;
}


void BuddyFileStore::buddy_buffer_head_t::encode(bufferlist& bl) const
{
#if 0
  ENCODE_START(1, 1, bl); // 이 숫자는 뭔지 잘 모르겠음. 
  ::encode(fname);
  ::encode(off);
  ::encode(bytes);
//  ::encode(data_bl);
  ENCODE_FINISH(bl);
#endif
}

void BuddyFileStore::buddy_buffer_head_t::decode(bufferlist::iterator& p)
{
#if 0
  DECODE_START(1, p);
  ::decode(fname);
  ::decode(off);
  ::decode(bytes);
  DECODE_FINISH(p);
#endif
}

ostream& operator<<(ostream& out, const BuddyFileStore::buddy_buffer_head_t& o)
{
  out << o.fname << ':';
  out << o.foff << ':';
  out << o.fbytes << ':'; 
  out << o.data_bl.length();
				        
  return out;
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

#ifdef EUNJI_TEST 
  buddy_log_entry_t* ble = new buddy_log_entry_t();

  bufferlist::iterator bp = bt.log_bl.begin();
  for (int i = 0; i < bt.log_entry_cnt; i++){
    ble->decode(bp);
    ldout(cct, 10) << "transaction op : cid = " << ble->cid << "op_type = " << ble->type << dendl;
  }
#endif
  /************
   * flush op 
   ***********/
  //buddy_cmt_lfo.alloc_space();



  /***********
   * flush data 
   ***********/
  for (vector<buddy_buffer_head_t>::iterator bhp = bt.bh_list.begin();
      bhp != bt.bh_list.end(); bhp++)
  {
    buddy_buffer_head_t& bh = *bhp;

    ldout(cct, 10) << "bh = " << bh << dendl;
    int r = bh.data_bl.write_file(bh.fname.c_str(), bh.foff, 0644);
    if (r) { // err
      ldout(cct, 10) << "Error in write "<< dendl;
      return -1;
    }

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
  }

  delete &bt;

#if 0

  for(bufferlist::iterator p = bt.log_bl.begin(); p != bt.log_bl.end(); p++)
  {
    ble->decode(p);
    ldout(cct, 10) << "op_type = " << ble->type << dendl;
  }

  for (vector<buddy_buffer_head_t>::iterator p = bt.bh_list.begin(); 
      p != bt.bh_list.end(); p++)
  {
    ldout(cct, 10) << "fname = " << p->fname << " foff = " << p->foff << dendl;

  }
#endif


//  delete bt;
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



/***************************
 * BuddyCollFileObject
 ****************************/

void BuddyFileStore::BuddyCollFileObject::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(tail_pos, bl);
  ::encode(buddy_object_map, bl);
  ENCODE_FINISH(bl);
}

void BuddyFileStore::BuddyCollFileObject::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(tail_pos, p);
  ::decode(buddy_object_map, p);
  DECODE_FINISH(p);
}


int BuddyFileStore::BuddyCollFileObject::insert_object_index (const ghobject_t& oid, const buddy_index_t& oindex)
{
      
  ldout(cct, 10) << __func__ << " oid = " << oid << " oindex = " << oindex << dendl;
  map<uint64_t, buddy_index_t> lmap;
  map<int, map<uint64_t, buddy_index_t>> tmap; // k: type 

  auto omap_entry = buddy_object_index.find(oid);
  if(omap_entry != buddy_object_index.end()){ // object map exists
    auto tmap_entry = omap_entry->second.find(oindex.type);
      
    if(tmap_entry != omap_entry->second.end()){ // type map exist 
      tmap_entry->second.insert(make_pair(oindex.ooff, oindex));
      return 0;
    }
    //else{ // type map does not exist 
    lmap.insert(make_pair(oindex.ooff,oindex));
    omap_entry->second.insert(make_pair(oindex.type, lmap)); 
    return 0;
  }
	
  // nothing found  
  lmap.insert(make_pair(oindex.ooff,oindex));
  tmap.insert(make_pair(oindex.type,lmap));
  buddy_object_index.insert(make_pair(oid, tmap));

  // insert object set 
  buddy_object_map.insert(oindex);

  return 0;
}


int BuddyFileStore::BuddyCollFileObject::delete_object_index (int type, const ghobject_t& oid, 
    const uint64_t ooff, const size_t bytes) 
{

  int index_found = 0;

  // 1. check map info 
  auto map_info = buddy_object_index.find(oid);
  // @return: map - first: ghobject_t, second: map 
    
  if (map_info != buddy_object_index.end())
  {
    ldout(cct, 10) << "object found" << dendl;
    auto tmap_info = map_info->second.find(type);
    // @return: map - first: comp_t, second; map 

    if (tmap_info != map_info->second.end())
    { // type found  

      ldout(cct, 10) << "type found" << dendl;

      uint64_t curr_ooff = ooff;
      size_t curr_bytes = bytes;
      uint64_t os, oe, ns, ne;

      // extent 가 많은 경우 map이 key를 기준으로 range search 되면 그거 쓰기. 
      for (map<uint64_t, buddy_index_t>::iterator p = tmap_info->second.begin(); 
	    p != tmap_info->second.end(); p++)
      {
	    os = p->second.ooff;
	    oe = p->second.ooff + p->second.used_bytes - 1;
	    ns = curr_ooff;
	    ne = curr_ooff + curr_bytes - 1;


	    // 1. include 
	    if (ns >= os && ne <= oe){
	      index_found = 1;
	      ldout(cct, 10) << "index found : include" << dendl;

	      // erase 
	      tmap_info->second.erase(p->first);
	      buddy_object_map.erase(p->second);

	      // first piece
	      buddy_index_t ni1 (oid, type, os, p->second.foff, ns - os, ns - os);
	      tmap_info->second.insert(make_pair(os, ni1));

	      // second piece 
    	      buddy_index_t ni2 (oid, type, ne+1, p->second.foff+(ne-os+1), oe-ne, oe-ne);
	      tmap_info->second.insert(make_pair(os, ni2));

	      // add_garbage();
	      break;
	    } 
	    // 2. head_overlap 
	    else if (ne >= os && ne <= oe){
	      index_found = 2;
	      ldout(cct, 10) << "index found : head_overlap" << dendl;
	      tmap_info->second.erase(p->first);
	      buddy_object_map.erase(p->second);

	      // second piece 
    	      buddy_index_t ni1 (oid, type, ne+1, p->second.foff+(ne-os+1), oe-ne, oe-ne);
	      tmap_info->second.insert(make_pair(os, ni1));

	      // add_garbage();
	      break;
	    }
	    // 3. tail_overlap 
	    else if (ns >= os && ns <= oe){
	      index_found = 3;
	      ldout(cct, 10) << "index found : tail_overlap" << dendl;

	      tmap_info->second.erase(p->first);
	      buddy_object_map.erase(p->second);

	      buddy_index_t ni1 (oid, type, os, p->second.foff, ns - os, ns - os);
	      tmap_info->second.insert(make_pair(os, ni1));

	      // add_garbage();

	      // forward offset 
	      curr_ooff = oe + 1; 
	      curr_bytes = curr_bytes - (oe-ns+1);
	    }
	} // end of for loop through type maps 
      } // end of type map found 
    } // end of object found 

  return index_found; 

}


int BuddyFileStore::BuddyCollFileObject::alloc_space (int type, 
    const ghobject_t& oid, const uint64_t ooff, const size_t bytes, 
    uint64_t& foff, size_t& alloc_bytes)
{
    ldout(cct, 10) << __func__ << " type: " << type << " oid: " << oid << "ooff: " << ooff << "bytes: " << bytes << dendl; 

    int r = delete_object_index(type, oid, ooff, bytes);
#if 0
    // 1. check map info 
    auto map_info = buddy_object_index.find(oid);
    // @return: map - first: ghobject_t, second: map 

    if (map_info != buddy_object_index.end()){ // object found 

      ldout(cct, 10) << "object found" << dendl;

      auto tmap_info = map_info->second.find(type); // iterator: first:
      // @return: map - first: comp_t, second; map 
      //
      ldout(cct, 10) << "type found" << dendl;

      if (tmap_info != map_info->second.end()){ // type found  
	//dout(10) << " EUNJI " << __func__ << " overalapped extent " << dendl; 

	uint64_t curr_ooff = ooff;
	size_t curr_bytes = bytes;
	uint64_t os, oe, ns, ne;

	// extent 가 많은 경우 map이 key를 기준으로 range search 되면 그거 쓰기. 
	for (map<uint64_t, buddy_index_t>::iterator p = tmap_info->second.begin(); 
	    p != tmap_info->second.end(); p++){

	    os = p->second.ooff;
	    oe = p->second.ooff + p->second.used_bytes - 1;
	    ns = curr_ooff;
	    ne = curr_ooff + curr_bytes - 1;


	    // 1. include 
	    if (ns >= os && ne <= oe){
	      ldout(cct, 10) << "extent found : include" << dendl;
	      // first piece
	      buddy_index_t ni1 (oid, type, os, p->second.foff, ns - os, ns - os);
	      tmap_info->second.erase(p->first);
	      tmap_info->second.insert(make_pair(os, ni1));
	      //(*p).erase(p->first);
	      //(*p).insert(make_pair(os, ni1));

	      // second piece 
    	      buddy_index_t ni2 (oid, type, ne+1, p->second.foff+(ne-os+1), oe-ne, oe-ne);
	      tmap_info->second.erase(p->first);
	      tmap_info->second.insert(make_pair(os, ni2));

	      // add_garbage();
	      break;
	    } 
	    // 2. head_overlap 
	    else if (ne >= os && ne <= oe){

	      ldout(cct, 10) << "extent found : head_overlap" << dendl;
	      // second piece 
    	      buddy_index_t ni1 (oid, type, ne+1, p->second.foff+(ne-os+1), oe-ne, oe-ne);
	      tmap_info->second.erase(p->first);
	      tmap_info->second.insert(make_pair(os, ni1));

	      // add_garbage();
	      break;
	    }
	    // 3. tail_overlap 
	    else if (ns >= os && ns <= oe){

	      ldout(cct, 10) << "extent found : tail_overlap" << dendl;

	      buddy_index_t ni1 (oid, type, os, p->second.foff, ns - os, ns - os);

	      tmap_info->second.erase(p->first);
	      tmap_info->second.insert(make_pair(os, ni1));

	      // add_garbage();

	      // forward offset 
	      curr_ooff = oe + 1; 
	      curr_bytes = curr_bytes - (oe-ns+1);
	    }
	} // end of for loop through type maps 
      } // end of type map found 
    } // end of object found 

    // 이미 할당한 공간에 붙일 수 있는지 체크. 겹치는 게 없었을 경우에만 해당하나? 
    // 아니지. 겹치는 경우애도.. 가능.. 사실 아예 겹치면. shadow 에 써야하는딩. 
    // 일단은 새로운거 들어오면 뒤에 붙이는걸로 해보자. 
    //
#endif

    //uint64_t alloc_bytes; 
    //
    ldout(cct, 10) << __func__ << "before allocation: tail_pos = " << tail_pos << dendl;

    if(last_alloc_oindex && bytes < (last_alloc_oindex->alloc_bytes - last_alloc_oindex->used_bytes)){
      // alloc space in last_alloc_oindex
      foff = last_alloc_oindex->foff + last_alloc_oindex->used_bytes;
      alloc_bytes = last_alloc_oindex->alloc_bytes - last_alloc_oindex->used_bytes;
      last_alloc_oindex->alloc_bytes = last_alloc_oindex->used_bytes;
    }else {
      // alloc new space 
      foff = tail_pos;
      alloc_bytes = ((bytes >> BUDDY_FILE_BSIZE_BITS) + 1) << BUDDY_FILE_BSIZE_BITS;
      tail_pos += alloc_bytes;
      assert (!(tail_pos & ((1 << BUDDY_FILE_BSIZE_BITS)-1)));
    } // end of allocation 

    ldout(cct, 10) << __func__ << " after allocation: tail_pos = " << tail_pos << " alloc_bytes = "<< alloc_bytes << dendl;

    ldout(cct, 10) << __func__ << " foff = " << foff << "used_bytes = " << bytes << "alloc_bytes = " << alloc_bytes << "pos = " << tail_pos << dendl;
    // insert map info 
    
    buddy_index_t* ni = new buddy_index_t(oid, type, ooff, foff, bytes, alloc_bytes);
    last_alloc_oindex = ni; // 이게 주소 그대로일지가 궁금하네- 변경하면 last_alloc_oindex 도 변경되는지 보면 될듯. 
    insert_object_index(oid, *ni);

    // insert object map
    buddy_object_map.insert(*ni);

    return 0;
}


/**************************
 * BuddyLogFileObject
 **************************/
int BuddyFileStore::BuddyLogFileObject::alloc_log_space (const uint64_t ooff, 
    const size_t bytes, uint64_t& foff, size_t& alloc_bytes)
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



/*************************************
 *  do_checkpoint() : flush all map 
 *  checkpoint buddystore 
 *************************************/

int BuddyFileStore::buddy_do_checkpoint()
{

  // sync metadata: object_map, coll_map, superblock  
  ldout(cct, 10) << __func__ << dendl;

  size_t bytes =0, alloc_bytes = 0; 
  uint64_t ooff=0, foff;
  size_t pos = 0;


  // write collections   
  map<coll_t, uint64_t> collections; // coll : coll_meta's offset 
  bufferlist meta_bl; // collection 각각의 object map 을 encode 해서 들고 있을 bufferlist 

  for (map<coll_t, BuddyCollFileObject>::iterator p = buddy_coll_fo.begin();
     p != buddy_coll_fo.end(); p++)
  { 
    p->second.encode(meta_bl);

    ldout(cct, 10) << " cid : obj_num = " << p->first << " : " << 
      p->second.buddy_object_index.size() << dendl;

    auto result = collections.insert(make_pair(p->first, pos));
    if(!result.second){
      ldout(cct, 10) << "collection insert failed" << dendl;
    }

    bytes = meta_bl.length() - bytes;
    pos += bytes; 
  }

  ::encode(collections, meta_bl);
  bytes = meta_bl.length() - bytes;
  ldout(cct, 10) << "collections location: pos = " << pos << " len = " << bytes << dendl;

  int r = buddy_meta_lfo.alloc_log_space(ooff, bytes, foff, alloc_bytes);

  if (r) {
    ldout(cct, 10) << "Failed to alloc meta space" << dendl;
    return r;
  }
 

  // write omap
  ldout(cct, 10) << "omap allocated at " << foff << " length " << alloc_bytes << dendl;
  r = meta_bl.write_file(buddy_meta_lfo.fname.c_str(), foff);

  if (r < 0) {
    ldout(cct, 10) << "Error in write_metafile with " << r << dendl;
    return r;
  }

  // flush superblock 
  buddy_sb.last_cp_off = foff; // collections' position 
  buddy_sb.last_cp_len = alloc_bytes;
  buddy_sb.last_cp_coll_off = foff + pos; 
  buddy_sb.last_cp_coll_len = bytes;
  buddy_sb.last_cp_version++; 

  r = write_buddy_sb();

  if (r < 0) {
    ldout(cct, 10) << "Error in write_buddy_sb with " << r << dendl;
    return r;
  }

  return 0;
}


/********************************
 * superblock operations 
 ********************************/
int BuddyFileStore::write_buddy_sb()
{
  ldout(cct,10) << __func__ << buddy_sb << dendl; 
  bufferlist bl;
//  ::encode(buddy_sb, bl);
  buddy_sb.encode(bl);
  assert(bl.length() < (1 << BUDDY_FILE_BSIZE_BITS)); 
  ldout(cct,10) << __func__ << " bl.length: " << bl.length() << dendl; 

  string fn = buddy_sb.fname;
  int r = bl.write_file(fn.c_str());

  ldout(cct,10) << __func__ << " wrote: " << r << dendl; 

  return r;
}

int BuddyFileStore::read_buddy_sb()
{

  ldout(cct,10) << __func__ << dendl; 

#if 0
  bufferlist bl;
  string err;
  
  int r = bl.read_file(buddy_sb.fname.c_str(), &err);
  if (r < 0) {
    ldout(cct, 10) << "Failed to read buddy_sb : " << err << dendl;
    return r;
  }

  bufferlist::iterator i = bl.begin();
  buddy_sb.decode(i);

  return 0;

#endif
  //bufferlist::iterator p;
  bufferptr bp(BUDDY_FILE_BSIZE); // < 4KB

  int r = safe_read_file(basedir.c_str(), "buddy_sb", 
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

  buddy_sb.decode(i);

  ldout(cct,10) << __func__ << " last_cp_off = " << buddy_sb.last_cp_off
    << " last_cp_version = " << buddy_sb.last_cp_version << dendl; 
  return 0;
}


/********************************
 * mount / umount 
 ********************************/

int BuddyFileStore::buddy_load()
{

  ldout(cct,10) << __func__ << dendl; 

  int r;
  // 1. read superblock 
  r = read_buddy_sb();
  if (r < 0) {
    ldout(cct, 10) << "Error in read_buddy_sb" << dendl;
    return r;
  }
 
  // 2. read collections

  ldout(cct, 10) << __func__ << "read collections " << dendl;
  bufferlist bl;
  string err;

  r = bl.read_file (
      buddy_meta_lfo.fname.c_str(), 
      buddy_sb.last_cp_coll_off,
      buddy_sb.last_cp_coll_len,
      &err
      ); // read whole file 
  if (r < 0) {
    ldout(cct, 10) << "Error in read collections" << dendl;
    return r;
  }

  if (bl.length() == 0){
    ldout(cct, 10) << "Empty file" << dendl;
    return r;
  }

  ldout(cct, 10) << "read = " << bl.length() << dendl;
   
  map<coll_t, uint64_t> collections;
  bufferlist::iterator p = bl.begin();
  ::decode(collections, p);



  // 3. buddy_coll_fo and buddy_object_map
  ldout(cct, 10) << "Read object map " << dendl;

  bufferlist fo_bl;

  r = fo_bl.read_file (
    buddy_meta_lfo.fname.c_str(), 
    buddy_sb.last_cp_off,
    buddy_sb.last_cp_len,
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

  bufferlist::iterator fp = fo_bl.begin();

  // create BuddyCollFileObject 
  for(map<coll_t, uint64_t>::iterator cp = collections.begin();
      cp != collections.end(); cp++)
  {

  // build buddy_coll_fo
    BuddyCollFileObject* coll_fo = new BuddyCollFileObject(cct, basedir, cp->first);
    coll_fo->decode(fp);


    /// build buddy_object_index 
    for(set<buddy_index_t>::iterator op = coll_fo->buddy_object_map.begin(); 
	op != coll_fo->buddy_object_map.end(); op++){
      coll_fo->insert_object_index((*op).oid, (*op));
    }

    // insert buddy_coll_fo
    auto result = buddy_coll_fo.insert(make_pair(cp->first, (*coll_fo)));
    assert(result.second);

    ldout(cct, 10) << (*cp) << " has " << coll_fo->buddy_object_map.size() << " objects" << dendl;
  }
  
  return 0;

}

int BuddyFileStore::mount()
{
  buddy_load();
  return 0;
}

int BuddyFileStore::umount()
{
  buddy_do_checkpoint();
  return 0;
}
