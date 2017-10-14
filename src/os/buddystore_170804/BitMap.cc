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


#include "BitMap.h"

string BitMap::sprint_bitmap_index(uint64_t index)
{
  return sprint_bitmap(bitmap[index]);
}

void BitMap::set_bitmap_index(uint64_t off, size_t count)
{

  uint64_t sidx = off / BITMAP_ENTRY_BITS;
  uint64_t eidx = sidx + count -1;
  uint64_t mask = 0; 

  /////
  uint64_t curr_off = off;
  uint64_t i, j;

  for (i = sidx; i <= eidx; i++) {
    for (j = curr_off % BITMAP_ENTRY_BITS; j < BITMAP_ENTRY_BITS; j++){
      mask |= (uint64_t) 1 << j; 
      curr_off++;
      count--;

      if(count == 0)
	break;
    }
  }
}


void BitMap::set_bitmap_blk(uint64_t off_bytes, size_t bytes)
{
  assert(!(off_bytes & (BLK_SIZE - 1)));
  assert(!(bytes & (BLK_SIZE - 1)));

  uint64_t sidx = (off_bytes / BLK_SIZE) / BITMAP_ENTRY_BITS;
  uint64_t eidx = ((off_bytes + bytes - 1) / BLK_SIZE) / BITMAP_ENTRY_BITS;
  uint64_t count = bytes / BLK_SIZE;

  uint64_t mask = 0; 

  /////
  uint64_t curr_off = off_bytes;
  uint64_t i, j;

  for (i = sidx; i <= eidx; i++) {
    for (j = curr_off % BITMAP_ENTRY_BITS; j < BITMAP_ENTRY_BITS; j++){
      mask |= (uint64_t) 1 << j; 
      curr_off++;
      count--;

      if(count == 0)
	break;
    }
  }
}

void BitMap::clear_bitmap_index(uint64_t off, size_t count)
{
  uint64_t sidx = off / BITMAP_ENTRY_BITS;
  uint64_t eidx = sidx + count -1;
  uint64_t mask = 0; 

  /////
  uint64_t curr_off = off;
  uint64_t i, j;

  for (i = sidx; i <= eidx; i++) {
    for (j = curr_off % BITMAP_ENTRY_BITS; j < BITMAP_ENTRY_BITS; j++){
      mask |= (uint64_t) 1 << j; 
      curr_off++;
      count--;

      if(count == 0)
	break;
    }
  }
}


void BitMap::clear_bitmap_blk(uint64_t off_bytes, size_t bytes)
{
  assert(!(off_bytes & (BLK_SIZE - 1)));
  assert(!(bytes & (BLK_SIZE - 1)));

  uint64_t sidx = (off_bytes / BLK_SIZE) / BITMAP_ENTRY_BITS;
  uint64_t eidx = ((off_bytes + bytes - 1) / BLK_SIZE) / BITMAP_ENTRY_BITS;
  uint64_t count = bytes / BLK_SIZE;

  uint64_t mask = 0; 

  /////
  uint64_t curr_off = off_bytes;
  uint64_t i, j;

  for (i = sidx; i <= eidx; i++) {
    for (j = curr_off % BITMAP_ENTRY_BITS; j < BITMAP_ENTRY_BITS; j++){
      mask |= (uint64_t) 1 << j; 
      curr_off++;
      count--;

      if(count == 0)
	break;
    }
  }
}


int BitMap::init_bitmap()
{
  uint64_t entry = (file_size / BLK_SIZE) / BITMAP_ENTRY_BITS; 

  while(entry--){
    uint64_t bit_entry = 0;
    bitmap.push_back(bit_entry);
  }
  return bitmap.size();
}

//
//int main()
//{
//
//  BitMap fo(1048576);
//
//  cout << fo.sprint_bitmap_index(0) << endl;
//  fo.set_bitmap(0, 8192);
//  fo.clear_bitmap(4096, 4096);
//
//  return 0;
//
//}
