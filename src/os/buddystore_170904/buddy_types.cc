
#include "buddy_types.h"

ostream& operator<< (ostream& out, const buddy_hindex_t& bi)
{
  out << bi.ctype << ':';
  out << bi.hoff << ':';
  out << bi.used_bytes << ':';
  out << bi.foff << ':';
  out << bi.alloc_bytes;

  return out;
}

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


