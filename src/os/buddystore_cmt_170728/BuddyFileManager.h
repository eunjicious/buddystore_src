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


#ifndef CEPH_BUDDYSTOREIO_H
#define CEPH_BUDDYSTOREIO_H

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
#if defined(__linux__)
#include <linux/fs.h>
#endif


//class FSSuperblock {
//  public:
//      CompatSet compat_features;
//      string omap_backend;
//
//      FSSuperblock() { } 
//
//      void encode(bufferlist &bl) const;
//      void decode(bufferlist::iterator &bl);
//      void dump(Formatter *f) const;
//      static void generate_test_instances(list<FSSuperblock*>& o); 
//};
//WRITE_CLASS_ENCODER(FSSuperblock)
//

class BuddyFileManager {


protected:
  enum elem_t { 
    data = 1, 
    xattr, 
    omap_header, 
    omap_keys, 
  };

  // Location of each elements of an object 
  struct map_info {
    elem_t type; // data, xattr, omap_header, omap_keys
    uint32_t off;
    uint32_t len;
  };

  /*********************************
  * BDTransaction 
  *
  * A BDTransaction represents a bunch of bufferlist to be written to 
  * multiple files. Each bufferlist holds the location information where they are going to be
  * recorded in a form of pair <fname, offset>
  *
  **********************************/ 

  class BDTransaction {

    // a list of bufferlist with location information 
    struct bl_head {
      string fname;
      uint64_t off;
      bufferlist bl; 
    };
  

  public:
    vector<bl_head> data_bl_h;

    BDTransaction () {};
    ~BDTransaction () {}; 

  };

  BuddyFileManager () {};
  ~BuddyFileManager () {};

};

#endif
