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

#include "BuddyStore.h"

#if 0
#define dout_context cct
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddystore(" << path << ") "
#endif

void BuddyFileStore::buddy_omap_entry_t::encode(bufferlist& bl) const
{
#if 0
  ENCODE_START(1, 1, bl); // 이 숫자는 뭔지 잘 모르겠음. 
  //ghobject_t 를 같이 저장할지 말지는 조금 더 고민. map 에 어차피 저장함.
  ::encode(type, bl);
  ::encode(ooff, bl);
  ::encode(foff, bl);
  ::encode(used_bytes);
  ::encode(alloc_bytes);
  ENCODE_FINISH(bl);
#endif
}

void BuddyFileStore::buddy_omap_entry_t::decode(bufferlist::iterator& p)
{
#if 0
  DECODE_START(1, p);
  ::decode(type, bl);
  ::decode(ooff, bl);
  ::decode(foff, bl);
  ::decode(used_bytes);
  ::decode(alloc_bytes);
  DECODE_FINISH(p);
#endif
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


// 
int BuddyFileStore::BuddyTransaction::prepare_commit()
{

  // sync 필요하면 sync 불러줌. 예를 들면 mkcoll 같은거. 
  return 0;
}



// 
int BuddyFileStore::BuddyTransaction::do_commit()
{
  return 0;
}

// 
int BuddyFileStore::BuddyTransaction::end_commit()
{

  // remove removed files 
  //
  return 0;
}


int BuddyFileStore::BuddyFileObject::insert_bmi (const ghobject_t& oid, const buddy_omap_entry_t& bmi)
{
      
  map<uint64_t, buddy_omap_entry_t> lmap;
  map<comp_t, map<uint64_t, buddy_omap_entry_t>> tmap;

  auto omap_entry = buddy_omap.find(oid);
  if(omap_entry != buddy_omap.end()){ // object map exists
    auto tmap_entry = omap_entry->second.find(bmi.type);
      
    if(tmap_entry != omap_entry->second.end()){ // type map exist 
      tmap_entry->second.insert(make_pair(bmi.ooff, bmi));
      return 0;
    }
    //else{ // type map does not exist 
    lmap.insert(make_pair(bmi.ooff,bmi));
    omap_entry->second.insert(make_pair(bmi.type, lmap)); 
    return 0;
  }
	
  // nothing found  
  lmap.insert(make_pair(bmi.ooff,bmi));
  tmap.insert(make_pair(bmi.type,lmap));
  buddy_omap.insert(make_pair(oid, tmap));

  return 0;
}

int BuddyFileStore::BuddyFileObject::get_fspace (comp_t type, 
    const ghobject_t& oid, const uint64_t ooff, const uint64_t bytes, 
    uint64_t& foff ) 
{
    //dout(10) << " EUNJI " << __func__ << " type: " << type << " oid: " << oid << "ooff: " << ooff << "bytes: " << bytes << dendl; 

    // 1. check map info 
    auto map_info = buddy_omap.find(oid);
    // @return: map - first: ghobject_t, second: map 

    if (map_info != buddy_omap.end()){ // object found 

      auto tmap_info = map_info->second.find(type); // iterator: first:
      // @return: map - first: comp_t, second; map 

      if (tmap_info != map_info->second.end()){ // type found  
	//dout(10) << " EUNJI " << __func__ << " overalapped extent " << dendl; 

	uint64_t curr_ooff = ooff;
	uint64_t curr_bytes = bytes;
	uint64_t os, oe, ns, ne;

	// extent 가 많은 경우 map이 key를 기준으로 range search 되면 그거 쓰기. 
	for (map<uint64_t, buddy_omap_entry_t>::iterator p = tmap_info->second.begin(); 
	    p != tmap_info->second.end(); p++){

	    os = p->second.ooff;
	    oe = p->second.ooff + p->second.used_bytes - 1;
	    ns = curr_ooff;
	    ne = curr_ooff + curr_bytes - 1;


	    // 1. include 
	    if (ns >= os && ne <= oe){
	      // first slice
	      buddy_omap_entry_t ni1 (type, os, p->second.foff, ns - os, ns - os);
	      tmap_info->second.erase(p->first);
	      tmap_info->second.insert(make_pair(os, ni1));
	      //(*p).erase(p->first);
	      //(*p).insert(make_pair(os, ni1));

	      // second slice 
    	      buddy_omap_entry_t ni2 (type, ne+1, p->second.foff+(ne-os+1), oe-ne, oe-ne);
	      tmap_info->second.erase(p->first);
	      tmap_info->second.insert(make_pair(os, ni2));

	      // add_garbage();
	      break;
	    } 
	    // 2. head_overlap 
	    else if (ne >= os && ne <= oe){
	      // second slice 
    	      buddy_omap_entry_t ni1 (type, ne+1, p->second.foff+(ne-os+1), oe-ne, oe-ne);
	      tmap_info->second.erase(p->first);
	      tmap_info->second.insert(make_pair(os, ni1));

	      // add_garbage();
	      break;
	    }
	    // 3. tail_overlap 
	    else if (ns >= os && ns <= oe){
	      buddy_omap_entry_t ni1 (type, os, p->second.foff, ns - os, ns - os);

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

    uint64_t alloc_bytes; 

    if(last_bmi && bytes < (last_bmi->alloc_bytes - last_bmi->used_bytes)){
      // alloc space in last_bmi
      foff = last_bmi->foff + last_bmi->used_bytes;
      alloc_bytes = last_bmi->alloc_bytes - last_bmi->used_bytes;
      last_bmi->alloc_bytes = last_bmi->used_bytes;
    }else {
      // alloc new space 
      foff = pos;
      alloc_bytes = ((bytes >> BUDDY_FILE_BSIZE_BITS) + 1) << BUDDY_FILE_BSIZE_BITS;
      pos += alloc_bytes;
      assert (!(pos & ~((1 << BUDDY_FILE_BSIZE_BITS)-1)));
    } // end of allocation 


    //dout(10) << " EUNJI " << __func__ << "alloc: " << "foff = " << foff << "used_bytes = " << bytes << "alloc_bytes = " << alloc_bytes << "pos = " << pos << dendl;
    // insert map info 
    buddy_omap_entry_t* ni = new buddy_omap_entry_t(type, ooff, foff, bytes, alloc_bytes);
    last_bmi = ni; // 이게 주소 그대로일지가 궁금하네- 변경하면 last_bmi 도 변경되는지 보면 될듯. 
    insert_bmi(oid, *ni);

    return 0;
}


