
#include "buddy_types.h"


#if 0
bool operator< (const buddy_lkey_t& b1, buddy_lkey_t& b2)
{
  off_t h1 = get_hash(b1.oid);
  off_t h2 = get_hash(b2.oid);
  
  if (h1 == h2)
    return b1.ooff < b2.ooff;
  else
    return h1 < h2; 
}

bool operator> (const buddy_lkey_t& b1, buddy_lkey_t& b2)
{
  off_t h1 = get_hash(b1.oid);
  off_t h2 = get_hash(b2.oid);
 
  if (h1 == h2)
    return b1.ooff > b2.ooff;
  else
    return h1 > h2; 
}


bool operator == (const buddy_lkey_t& b1, buddy_lkey_t& b2)
{
  off_t h1 = get_hash(b1.oid);
  off_t h2 = get_hash(b2.oid);

  return (h1 == h2) && (b1.ooff == b2.ooff);
}

ostream& operator<< (ostream& out, const buddy_lkey_t& bi)
{
  out << bi.oid << ':';
  out << bi.ooff;

  return out;
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

#endif

 uint32_t _reverse_bits(uint32_t v) {
   if (v == 0)
   
	return v;
 
   // reverse bits
   // swap odd and even bits
   v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
   // swap consecutive pairs
   v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
   // swap nibbles ...
   v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
   // swap bytes
   v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
   // swap 2-byte long pairs
   v = ( v >> 16             ) | ( v               << 16);
   return v;
 }

 off_t round_up(off_t v){
  
  if( v % 4096 == 0)
      return v; 
   return ((v >> BUDDY_FALLOC_SIZE_BITS)+1) << BUDDY_FALLOC_SIZE_BITS;
 }


 off_t round_down(off_t v){
   return (v >> BUDDY_FALLOC_SIZE_BITS) << BUDDY_FALLOC_SIZE_BITS;
 }

 off_t hash_to_hoff(const ghobject_t& oid){
   uint64_t bnum = (uint64_t)_reverse_bits(oid.hobj.get_hash());
   return bnum << BUDDY_FALLOC_SIZE_BITS; // hash(blk num) to bytes 
 }
 
 off_t get_hash(const ghobject_t& oid){
   return (uint64_t)_reverse_bits(oid.hobj.get_hash());
 }




ostream& operator<< (ostream& out, const buddy_iov_t& iov)
{
  out << iov.fname << ':';
  out << iov.off_in_src << ':';
  out << iov.bytes << ':';
  out << iov.foff << ':';
  out << iov.data_bl.length();

  return out;
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


ostream& operator<< (ostream& out, const buddy_index_t& idx)
{
  out << idx.ooff << ':';
  out << idx.foff << ':';
  out << idx.used_bytes << ':';
  out << idx.boff << ':';
  out << idx.alloc_bytes << ':';

  return out;
}

bool operator< (const buddy_iov_t& b1, const buddy_iov_t& b2)
{
  return b1.foff < b2.foff;
}

bool operator> (const buddy_iov_t& b1, const buddy_iov_t& b2)
{
  return b1.foff > b2.foff;
}


bool operator == (const buddy_iov_t& b1, const buddy_iov_t& b2)
{
  return b1.foff == b2.foff;
}


