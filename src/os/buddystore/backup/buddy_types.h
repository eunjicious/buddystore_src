/// 위의 클래스를 BuddyFileStore 안에 넣어도 되나. 
//

#ifndef CEPH_BUDDY_TYPES_H
#define CEPH_BUDDY_TYPES_H

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

// Location of each elements of an object 
// 해당 hash 위치에서 data 를 관리하는 자료구조
// 이 정보는 나중에 checkpoint 할떄 저장할 필요가 있나? 
// 각 파일에는 이 자료구조가 map 이든 vector 형태로 존재하겠지. 
// 최소한 object 마다 하나씩은 있으니까.  
//

/*************************
 *  buddy_hindex_t
 ************************/
struct buddy_hindex_t {

  ghobject_t oid;
  int ctype;
  off_t hoff;
//  off_t ooff; // = 0. remove later 
  off_t foff; // = hoff. remove later 
  ssize_t used_bytes;
  ssize_t alloc_bytes;
//  vector<uint64_t> sbitmap; // shadow 
//  vector<uint64_t> cbitmap; // collision 

  off_t get_alloc_end() {return hoff + alloc_bytes;}
  off_t get_used_end() {return hoff + used_bytes;}
  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& bl);

  friend ostream& operator<<(ostream& out, const buddy_hindex_t& o);
  friend bool operator < (const buddy_hindex_t& b1, const buddy_hindex_t& b2);
  friend bool operator > (const buddy_hindex_t& b1, const buddy_hindex_t& b2);


 // explicit buddy_hindex_t(ghobject_t oid_, int t, uint64_t oo, uint64_t fo, uint64_t ubytes, uint64_t abytes): 
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

void buddy_hindex_t::encode(bufferlist& bl) const
{
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

void buddy_hindex_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(oid, p);
  ::decode(ctype, p);
  ::decode(hoff, p);
  ::decode(foff, p);
  ::decode(used_bytes, p);
  ::decode(alloc_bytes, p);
  DECODE_FINISH(p);
}


bool operator< (const buddy_hindex_t& b1, const buddy_hindex_t& b2)
{
  return b1.oid < b2.oid;
}

bool operator> (const buddy_hindex_t& b1, const buddy_hindex_t& b2)
{
  return b1.oid < b2.oid;
}

ostream& operator<< (ostream& out, const buddy_hindex_t& bi)
{
  out << bi.ctype << ':';
  out << bi.hoff << ':';
  out << bi.used_bytes << ':';
  out << bi.foff << ':';
  out << bi.alloc_bytes;

  return out;
}


/*************************
 *  buddy_iov_t
 ************************/

struct buddy_iov_t {
  int ftype;
  string fname;
  uint64_t ooff; // 주어진 bl 에서의 offset 
  uint64_t foff;
  uint64_t bytes;
  bufferlist data_bl; 
 
  void encode(bufferlist& bl) const 
  {
    ENCODE_START(1, 1, bl);
    ::encode(fname, bl);
    ::encode(ooff, bl);
    ::encode(foff,bl);
    ::encode(bytes, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p)
  {
    DECODE_START(1, p);
    ::decode(fname, p);
    ::decode(ooff, p);
    ::decode(foff, p);
    ::decode(bytes, p);
    DECODE_FINISH(p);
  }

  friend ostream& operator<<(ostream& out, const buddy_iov_t& o);

  explicit buddy_iov_t(int ft, string fn, off_t oo, off_t fo, uint64_t b){
    ftype = ft;
    fname = fn;
    ooff = oo;
    foff = fo;
    bytes = b;
  }

  buddy_iov_t() : fname("init"), ooff(0), foff(0), bytes(0){}
  ~buddy_iov_t(){}

};
WRITE_CLASS_ENCODER(buddy_iov_t)

ostream& operator<< (ostream& out, const buddy_iov_t& iov)
{
  out << iov.ftype << ':';
  out << iov.fname << ':';
  out << iov.ooff << ':';
  out << iov.foff << ':';
  out << iov.bytes << ':';
  out << iov.data_bl.length();

  return out;
}



/*************************
 *  buddy_superblock_t
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
    
    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& p);

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


void buddy_superblock_t::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(last_cp_off, bl);
  ::encode(last_cp_len, bl);
  ::encode(last_cp_coll_off, bl);
  ::encode(last_cp_coll_len, bl);
  ::encode(last_cp_version, bl);
  ENCODE_FINISH(bl);
}


void buddy_superblock_t::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(last_cp_off, p);
  ::decode(last_cp_len, p);
  ::decode(last_cp_coll_off, p);
  ::decode(last_cp_coll_len, p);
  ::decode(last_cp_version, p);
  DECODE_FINISH(p);
}

ostream& operator<<(ostream& out, const buddy_superblock_t& o)
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

/*************************
 *  buddy_log_entry_t
 ************************/
  
struct buddy_log_entry_t {
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

   
  explicit buddy_log_entry_t(const coll_t& c, const ghobject_t& o, __le32 t, bufferlist& bl) {
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

WRITE_CLASS_ENCODER(buddy_log_entry_t)


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
