
#ifndef CEPH_BUDDYLOGDATAFILEOBJECT_H
#define CEPH_BUDDYLOGDATAFILEOBJECT_H


#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "include/memory.h"
#include "common/errno.h"
#include "common/RWLock.h"
//#include "BuddyStore.h"
#include "include/compat.h"
#include "buddy_types.h"


#define BUDDY_PREALLOC_SIZE 1UL << 32 // 16M
//#define BUDDY_PREALLOC_SIZE 1UL << 28 // 16M
#define BUDDY_PREALLOC_LOW_WATERMARK 1 << 20 // 1M



class BuddyLogDataFileObject {

  public:
    CephContext* cct;

    // file information 
    string fname;
    bool directio;
    int type;

    int dfd;
    off_t tail_off;
    off_t total_used_bytes;
    off_t total_alloc_bytes;
    off_t total_hole_bytes;
    off_t prealloc_bytes;
    off_t prewrite_unit_bytes;

    bool file_prewrite;
    bool file_prealloc; 
    bool file_inplace_write;

  private:
    // space management 
    uint64_t max_fbytes;
    uint64_t used_fbytes;


  public:
    Mutex lock; 
    //RWLock lock; // log_index_map lock 
    //map<ghobject_t, buddy_index_map_t> log_index_map;
    map<off_t, ssize_t> free_index_map; // foff. not ooff!!!!

    int create_or_open_file(int flag);
    int delete_file();
    int close_file();
    void stat_file();

    int alloc_space(coll_t cid, const ghobject_t& oid, const off_t ooff, const ssize_t bytes, 
      vector<buddy_iov_t>& iov);
    //int get_space_info(const ghobject_t& oid, const off_t ooff, const ssize_t bytes,
     //  vector<buddy_iov_t>& iov);
    // @return: -1 on fail 

    int release_space(const buddy_index_map_t& omap); 

    int truncate_space(const ghobject_t& oid, ssize_t size) {return 0;}
    int clone_space(const ghobject_t& ooid, const ghobject_t& noid, vector<buddy_iov_t>& iov){return 0;}
    int clone_space(const ghobject_t& ooid, const ghobject_t& noid, off_t srcoff, size_t bytes, 
	off_t dstoff, vector<buddy_iov_t>& iov) {return 0;}
    int write_fd(bufferlist& bl, uint64_t foff);
    void sync();

    //int read_fd(const ghobject_t& oid, bufferlist& bl, uint64_t foff, ssize_t size);
    int read_fd(bufferlist& bl, uint64_t foff, size_t size);
    int preallocate(uint64_t offset, size_t len);
  

    void encode(bufferlist& bl) const 
    {
      ENCODE_START(1, 1, bl);
      //::encode(log_index_map, bl);
      ::encode(tail_off, bl);
      ::encode(total_used_bytes, bl);
      ::encode(total_alloc_bytes, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator& p)
    {
      DECODE_START(1, p);
      //::decode(log_index_map, p);
      //::decode(tail_off, p);
      ::decode(tail_off, p);
      ::decode(total_used_bytes, p);
      ::decode(total_alloc_bytes, p);
      DECODE_FINISH(p);
    }
  
    BuddyLogDataFileObject(CephContext* cct_, string fname_, bool dio, bool prealloc);
 
    ~BuddyLogDataFileObject(){
    }
};


WRITE_CLASS_ENCODER(BuddyLogDataFileObject)

#endif
