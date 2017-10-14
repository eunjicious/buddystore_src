/// 위의 클래스를 BuddyFileStore 안에 넣어도 되나. 
//

#ifndef CEPH_BUDDY_TYPES_H
#define CEPH_BUDDY_TYPES_H

#include "include/types.h"
#include "include/buffer.h"
#include "osd/osd_types.h"

#define BUDDY_FALLOC_SIZE_BITS 12
#define BUDDY_FALLOC_SIZE 1 << 12


// 32KB
//#define BUDDY_FALLOC_SIZE_BITS 16
//#define BUDDY_FALLOC_SIZE 1 << 16

#if 0
const int N_COMPONENT = 4; // data, xattr. omap 은? 이건 logging 만하고 가끔씩 flush 만 하자.   

typedef const int ctype_t;

ctype_t BD_NONE_T = 0;
ctype_t BD_DATA_T = 1;
ctype_t BD_XATTR_T = 2;
ctype_t BD_OH_T = 3;
ctype_t BD_OM_T = 4;

typedef const uint32_t ftype_t;

ftype_t BD_ORIG_F = 0x001;
ftype_t BD_SHDW_F = 0x010;
ftype_t BD_COLL_F = 0x100;
#endif

/********************
 * utils 
 ***************/
 uint32_t _reverse_bits(uint32_t v);
 off_t round_up(off_t v);
 off_t round_down(off_t v);
 off_t hash_to_hoff(const ghobject_t& oid); 
 off_t get_hash(const ghobject_t& oid);

//////////////////////////////////////////


/*************************
 *  buddy_iov_t
 ************************/

struct buddy_iov_t {
  coll_t cid;
  ghobject_t oid;

  string fname;
//  int fd;
   
  uint64_t off_in_src; // osd 넘어온 data_bl 에서의 offset 
  uint64_t ooff;
  uint64_t foff; // address of actual data placement 
  uint64_t off_in_blk;

  uint64_t bytes;
  uint64_t alloc_bytes;

  bufferlist data_bl; 
 
  void encode(bufferlist& bl) const 
  {
    ENCODE_START(1, 1, bl);
    ::encode(fname, bl);
    ::encode(off_in_src, bl);
    ::encode(ooff, bl);
    ::encode(foff,bl);
    ::encode(off_in_blk, bl);
    ::encode(bytes, bl);
    ::encode(alloc_bytes, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p)
  {
    DECODE_START(1, p);
    ::decode(fname, p);
    ::decode(off_in_src, p);
    ::decode(ooff, p);
    ::decode(foff, p);
    ::decode(off_in_blk, p);
    ::decode(bytes, p);
    ::decode(alloc_bytes, p);
    DECODE_FINISH(p);
  }

  friend ostream& operator<<(ostream& out, const buddy_iov_t& o);
  friend bool operator < (const buddy_iov_t& b1, const buddy_iov_t& b2);
  friend bool operator > (const buddy_iov_t& b1, const buddy_iov_t& b2);
  friend bool operator == (const buddy_iov_t& b1, const buddy_iov_t& b2);


  //explicit buddy_iov_t(coll_t* cid_, int ft, string fn, off_t src_off, off_t fo, uint64_t b)
  explicit buddy_iov_t(coll_t cid_, const ghobject_t& oid_, string fn, off_t src_off, off_t _ooff, off_t _foff, uint64_t _bytes, uint64_t _alloc_bytes): cid(cid_)
  {
    oid = oid_;
    fname = fn;

    off_in_src = src_off;
    ooff = _ooff;
    foff = _foff;
    bytes = _bytes;
    alloc_bytes = _alloc_bytes;

    off_in_blk = foff - round_down(foff);
  }

  buddy_iov_t() : fname("init"), off_in_src (0), ooff(0), foff(0), bytes(0), alloc_bytes(0){}
  ~buddy_iov_t(){}

};
WRITE_CLASS_ENCODER(buddy_iov_t)


/*************************
 *  buddy_index_t
 ************************/

struct buddy_index_t {

  //ghobject_t oid;
  off_t ooff;
  off_t foff;
  ssize_t used_bytes;

  off_t boff; 
  ssize_t alloc_bytes;

  void encode(bufferlist& bl) const {

    ENCODE_START(1, 1, bl); // 이 숫자는 뭔지 잘 모르겠음. 
    ::encode(ooff, bl);
    ::encode(foff, bl);
    ::encode(used_bytes, bl);
    ::encode(boff, bl);
    ::encode(alloc_bytes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p){
    DECODE_START(1, p);
    ::decode(ooff, p);
    ::decode(foff, p);
    ::decode(used_bytes, p);
    ::decode(boff, p);
    ::decode(alloc_bytes, p);
    DECODE_FINISH(p);
  }

  friend ostream& operator<<(ostream& out, const buddy_index_t& o);
  //friend bool operator < (const buddy_index_t& b1, const buddy_index_t& b2);
  //friend bool operator > (const buddy_index_t& b1, const buddy_index_t& b2);

  explicit buddy_index_t(uint64_t oo, uint64_t fo, uint64_t ubytes): 
    ooff(oo),
    foff(fo),
    used_bytes(ubytes),
    boff(round_down(foff)),
    alloc_bytes(round_up(foff + used_bytes -1) - foff) {
  }
  buddy_index_t(){}
  ~buddy_index_t(){}
};
WRITE_CLASS_ENCODER(buddy_index_t)

/*************************
 *  buddy_index_map_t
 ************************/

struct buddy_index_map_t {
  map<off_t, buddy_index_t> index_map; // ooff 
  bufferlist *cache_bl;

  void encode(bufferlist& bl) const {

    ENCODE_START(1, 1, bl); // 이 숫자는 뭔지 잘 모르겠음. 
    ::encode(index_map, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p){
    DECODE_START(1, p);
    ::decode(index_map, p);
    DECODE_FINISH(p);
  }

  buddy_index_map_t(){}
  ~buddy_index_map_t(){}

};
WRITE_CLASS_ENCODER(buddy_index_map_t)



/*************************
 *  buddy_euperblock_t
 ************************/

class buddy_superblock_t {
public:
  string fname;
    uint64_t last_cp_off; // coll_map 
    size_t last_cp_len; // coll_map size
    uint64_t last_cp_coll_off;
    size_t last_cp_coll_len;
    int last_cp_version;
    // uint64_t checksum;
    
    void encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(last_cp_off, bl);
  ::encode(last_cp_len, bl);
  ::encode(last_cp_coll_off, bl);
  ::encode(last_cp_coll_len, bl);
  ::encode(last_cp_version, bl);
  ENCODE_FINISH(bl);
}


    void decode(bufferlist::iterator& p) {
  DECODE_START(1, p);
  ::decode(last_cp_off, p);
  ::decode(last_cp_len, p);
  ::decode(last_cp_coll_off, p);
  ::decode(last_cp_coll_len, p);
  ::decode(last_cp_version, p);
  DECODE_FINISH(p);
}


    friend ostream& operator<<(ostream& out, const buddy_superblock_t& o);

    explicit buddy_superblock_t(string basedir_, string fname_){
      fname = basedir_ + "/" + fname_;
      last_cp_off = 0;
      last_cp_len = 0;
      last_cp_coll_off = 0;
      last_cp_coll_len = 0;
      last_cp_version = 0;
    }
    buddy_superblock_t(){}
    ~buddy_superblock_t(){}
};
WRITE_CLASS_ENCODER(buddy_superblock_t)


// Location of each elements of an object 
// 해당 hash 위치에서 data 를 관리하는 자료구조
// 이 정보는 나중에 checkpoint 할떄 저장할 필요가 있나? 
// 각 파일에는 이 자료구조가 map 이든 vector 형태로 존재하겠지. 
// 최소한 object 마다 하나씩은 있으니까.  
// 근데 object 의 데이터가 흩어지면 어찌되나.. shadow 
// 여기서 관리를 아예 하는구먼.. 그래서 ooff 가 필요없었군.
// log structure 에선 

#if 0
/*************************
 *  buddy_hindex_t
 ************************/
struct buddy_hindex_t {

  ghobject_t oid;
  int ctype;
  off_t hoff;
//  off_t ooff; // = 0. remove later 
  off_t foff; // = hoff. remove later. log 에선 써야겠다.  
  ssize_t used_bytes;
  ssize_t alloc_bytes;
//  vector<uint64_t> sbitmap; // shadow 
//  vector<uint64_t> cbitmap; // collision 

  off_t get_alloc_end() {return hoff + alloc_bytes;}
  off_t get_used_end() {return hoff + used_bytes;}

// void encode(bufferlist& bl) const;
// void decode(bufferlist::iterator& bl);
//
  void encode(bufferlist& bl) const {

    ENCODE_START(1, 1, bl); // 이 숫자는 뭔지 잘 모르겠음. 
  //ghobject_t 를 같이 저장할지 말지는 조금 더 고민. map 에 어차피 저장함.
  ::encode(oid, bl);
  ::encode(ctype, bl);
  ::encode(hoff, bl);
  ::encode(foff, bl);
  ::encode(used_bytes, bl);
  ::encode(alloc_bytes, bl);
  ENCODE_FINISH(bl);

  }

  void decode(bufferlist::iterator& p){
  DECODE_START(1, p);
  ::decode(oid, p);
  ::decode(ctype, p);
  ::decode(hoff, p);
  ::decode(foff, p);
  ::decode(used_bytes, p);
  ::decode(alloc_bytes, p);
  DECODE_FINISH(p);
  }

  friend ostream& operator<<(ostream& out, const buddy_hindex_t& o);
  friend bool operator < (const buddy_hindex_t& b1, const buddy_hindex_t& b2);
  friend bool operator > (const buddy_hindex_t& b1, const buddy_hindex_t& b2);

  explicit buddy_hindex_t(ghobject_t oid_, int t, uint64_t ho, uint64_t ubytes, uint64_t abytes): 
    oid(oid_) {
    ctype = t;
    hoff = ho;
    used_bytes = ubytes;
    alloc_bytes = abytes;
  }

  buddy_hindex_t(){}
  ~buddy_hindex_t(){}
};
WRITE_CLASS_ENCODER(buddy_hindex_t)


/*************************
 *  buddy_lkey_t
 ************************/

struct buddy_lkey_t {
  ghobject_t oid;
  off_t ooff;

  void encode(bufferlist& bl) const {

    ENCODE_START(1, 1, bl); // 이 숫자는 뭔지 잘 모르겠음. 
    ::encode(oid, bl);
    ::encode(ooff, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p){
  DECODE_START(1, p);
  ::decode(oid, p);
  ::decode(ooff, p);
  DECODE_FINISH(p);
  }

  friend ostream& operator<<(ostream& out, const buddy_lkey_t& o);
  friend bool operator < (const buddy_lkey_t& b1, const buddy_lkey_t& b2);
  friend bool operator > (const buddy_lkey_t& b1, const buddy_lkey_t& b2);
  friend bool operator == (const buddy_lkey_t& b1, const buddy_lkey_t& b2);
}
#endif
/*************************
 *  buddy_log_item_t
 ************************/
  
struct buddy_log_item_t {
  coll_t cid;
  ghobject_t oid;
  __le32 type;
  bufferlist arg_bl;
 
  void encode(bufferlist& log_bl) const{
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

   
  explicit buddy_log_item_t(const coll_t& c, const ghobject_t& o, __le32 t, bufferlist& bl) {
    cid = c;
    oid = o;
    type = t;
    arg_bl = bl;
  } 
    
  explicit buddy_log_item_t(const coll_t& c, const ghobject_t& o, __le32 t)
  {
    cid = c;
    oid = o;
    type = t;
  } 
  
  buddy_log_item_t(){}
  ~buddy_log_item_t(){}
}; // end of struct BuddyOp

WRITE_CLASS_ENCODER(buddy_log_item_t)


/*************************
 *  buddy_log_entry_header_t
 ************************/
#if 0
struct buddy_log_header_t {
  uint64_t version;
//  uint64_t seq;     // fs op seq #
  uint32_t crc32c;  // payload only.  not header, pre_pad, post_pad, or footer.
  uint32_t len; // log payload's length 
  uint32_t op_num; // number of ops 
  uint32_t pre_pad, post_pad;
  uint64_t magic1;
  uint64_t magic2;

//  static uint64_t make_magic(uint64_t seq, uint32_t len, uint64_t fsid) {
//      return (fsid ^ seq ^ len);
//  }
//
  static uint64_t make_magic(uint64_t version, uint32_t len, uint64_t fsid) {
      return (fsid ^ version ^ len);
  }
  bool check_magic(off64_t pos, uint64_t fsid) {
      return
    magic1 == (uint64_t)pos &&
    magic2 == (fsid ^ version ^ len);
    //magic2 == (fsid ^ seq ^ len);
  }
} __attribute__((__packed__, aligned(4)));
#endif

#endif
