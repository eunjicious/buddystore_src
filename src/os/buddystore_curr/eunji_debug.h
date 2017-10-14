
#if 0
    enum {
      OP_NOP =          0,
      OP_TOUCH =        9,   // cid, oid
      OP_WRITE =        10,  // cid, oid, offset, len, bl
      OP_ZERO =         11,  // cid, oid, offset, len
      OP_TRUNCATE =     12,  // cid, oid, len
      OP_REMOVE =       13,  // cid, oid
      OP_SETATTR =      14,  // cid, oid, attrname, bl
      OP_SETATTRS =     15,  // cid, oid, attrset
      OP_RMATTR =       16,  // cid, oid, attrname
      OP_CLONE =        17,  // cid, oid, newoid
      OP_CLONERANGE =   18,  // cid, oid, newoid, offset, len
      OP_CLONERANGE2 =  30,  // cid, oid, newoid, srcoff, len, dstoff

      OP_TRIMCACHE =    19,  // cid, oid, offset, len  **DEPRECATED**

      OP_MKCOLL =       20,  // cid
      OP_RMCOLL =       21,  // cid
      OP_COLL_ADD =     22,  // cid, oldcid, oid
      OP_COLL_REMOVE =  23,  // cid, oid
      OP_COLL_SETATTR = 24,  // cid, attrname, bl
      OP_COLL_RMATTR =  25,  // cid, attrname
      OP_COLL_SETATTRS = 26,  // cid, attrset
      OP_COLL_MOVE =    8,   // newcid, oldcid, oid

      OP_STARTSYNC =    27,  // start a sync

      OP_RMATTRS =      28,  // cid, oid
      OP_COLL_RENAME =       29,  // cid, newcid

      OP_OMAP_CLEAR = 31,   // cid
      OP_OMAP_SETKEYS = 32, // cid, attrset
      OP_OMAP_RMKEYS = 33,  // cid, keyset
      OP_OMAP_SETHEADER = 34, // cid, header
      OP_SPLIT_COLLECTION = 35, // cid, bits, destination
      OP_SPLIT_COLLECTION2 = 36, /* cid, bits, destination
				    doesn't create the destination */
      OP_OMAP_RMKEYRANGE = 37,  // cid, oid, firstkey, lastkey
      OP_COLL_MOVE_RENAME = 38,   // oldcid, oldoid, newcid, newoid

      OP_SETALLOCHINT = 39,  // cid, oid, object_size, write_size
      OP_COLL_HINT = 40, // cid, type, bl

      OP_TRY_RENAME = 41,   // oldcid, oldoid, newoid

      OP_COLL_SET_BITS = 42, // cid, bits
    };

#endif

static const char * optable[] = {
  "OP_NOP", 
  "",  // 1
  "",  // 2
  "",  // 3
  "",  // 4
  "",  // 5
  "",  // 6
  "",  // 7
  "OP_COLL_MOVE", // 8
  "OP_TOUCH", // 9
  "OP_WRITE", // 10
  "OP_ZERO", // 11
  "OP_TRUNCATE", // 12
  "OP_REMOVE", // 13
  "OP_SETATTR", // 14
  "OP_SETATTRS", // 15
  "OP_RMATTR", // 16
  "OP_CLONE", // 17
  "OP_CLONERANGE", // 18
  "OP_TRIMCACHE",  // 19
  "OP_MKCOLL",  // 20
  "OP_RMCOLL",  // 21
  "OP_COLL_ADD",  // 22
  "OP_COLL_REMOVE",  // 23
  "OP_COLL_SETATTR",  // 24
  "OP_COLL_RMATTR",  // 25
  "OP_COLL_SETATTRS",  // 26
  "OP_COLL_MOVE",  // 27
  "OP_RMATTRS",  // 28
  "OP_COLL_RENAME",  // 29
  "OP_CLONERANGE2", // 30 
  "OP_OMAP_CLEAR", // 31 
  "OP_OMAP_SETKEYS", //32, // cid, attrset
  "OP_OMAP_RMKEYS", //33,  // cid, keyset
  "OP_OMAP_SETHEADER", //34, // cid, header
  "OP_SPLIT_COLLECTION", //35, // cid, bits, destination
  "OP_SPLIT_COLLECTION2", //36,
  "OP_OMAP_RMKEYRANGE", //37,  // cid, oid, firstkey, lastkey
  "OP_COLL_MOVE_RENAME", //38,   // oldcid, oldoid, newcid, newoid
  "OP_SETALLOCHINT", //39,  // cid, oid, object_size, write_size
  "OP_COLL_HINT", //40, // cid, type, bl
  "OP_TRY_RENAME", //41,   // oldcid, oldoid, newoid
  "OP_COLL_SET_BITS", //42, // cid, bits
};
 

















