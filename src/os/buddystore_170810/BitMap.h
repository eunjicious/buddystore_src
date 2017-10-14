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

#ifndef BUDDY_BITMAP
#define BUDDY_BITMAP

//#include<cstdint>
//#include<iostream>
#include<vector>
//#include<cmath>
//#include<inttypes.h>
//#include<cassert>

using namespace std;

class BitMap {

#if 0
  const uint64_t BITMAP_ENTRY_BITS  = sizeof(uint64_t) * 8; 
  const uint64_t BLK_SIZE_BIT = 12;
  const uint64_t BLK_SIZE = (uint64_t)1 << BLK_SIZE_BIT; 

  uint64_t bytes_per_bitmap_entry;
  uint64_t blks_per_bitmap_entry;

  private:
    uint64_t file_size;
    vector<uint64_t> bitmap; // allocation 

  public:
    int init_bitmap();
    void set_bitmap_blk(uint64_t off_bytes, size_t bytes); // file's offset and length in bytes 
    void set_bitmap_index(uint64_t off, size_t count); // bitmap index 
    void clear_bitmap_blk(uint64_t off_bytes, size_t bytes);
    void clear_bitmap_index(uint64_t off, size_t count);
    //int increae_bitmap();
    //int shrink_bitmap();
    //int truncate_bitmap(uint64_t file_size);

    string sprint_bitmap(uint64_t _bitmap);
    string sprint_bitmap_index(uint64_t index);
  
    BitMap(uint64_t init_file_size) : file_size(init_file_size) {
      init_bitmap();
    }
#endif
    BitMap(){}
    ~BitMap(){}
};

#endif

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
