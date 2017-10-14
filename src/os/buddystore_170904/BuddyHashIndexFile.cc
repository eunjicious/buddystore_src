#include "BuddyHashIndexFile.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddyfilestore " 


const static int64_t ONE_MEG(1 << 20);
const static int CEPH_DIRECTIO_ALIGNMENT(4096);
/**********************************
 *  BuddyHashIndexFile functions 
 **********************************/


/******
 * create_or_open_file  
 */
int BuddyHashIndexFile::create_or_open_file(int out_flags)
{
  ldout(cct,10) << __func__ << dendl;

  // 여기서 direct io 인 경우에 세티하도록 수정. 
  int flags = O_RDWR | O_CREAT;
  flags |= out_flags;
    
  if (directio) {
    ldout(cct,10) << __func__ << " open with O_DIRECT | O_DSYNC" << dendl;
    flags |= O_DIRECT | O_DSYNC;
  } else {
    ldout(cct, 10) << __func__ << " open without O_DIRECT | O_DSYNC" << dendl;
  }


  int r = ::open(fname.c_str(), flags, 0644);
  if (r < 0){
    ldout(cct,10) << "Failed to create file: " << fname << dendl; 
    return r;
  }
  hfd = r;

  // we can hold it for later .. 
  if(preallocation)
    preallocate(0, max_fbytes);

  ldout(cct,10) << __func__ << " fd " << hfd << dendl;

  //::close(fd);
  return 0;
}

/******
 * delete_file  
 */
int BuddyHashIndexFile::delete_file()
{
  ldout(cct,10) << __func__ << dendl;

  if (hfd > 0)
    close_file();

  int r = ::unlink(fname.c_str());
  if (r < 0){
    ldout(cct,10) << "Failed to delete file: " << fname << dendl; 
  }
  return 0;
}

/******
 * close_file
 */
int BuddyHashIndexFile::close_file()
{
  if (hfd < 0){
    ldout(cct, 10) << "no file exists" << dendl; 
    return -1;
  }

  //int r = ::close(fd);

  VOID_TEMP_FAILURE_RETRY(::close(hfd));
  return 0; 

}


/******
 * alloc_space 
 */

int BuddyHashIndexFile::alloc_space(int type, const ghobject_t& oid, 
    const off_t ooff, const ssize_t bytes, vector<buddy_iov_t>& iov)
{
  off_t hoff = hash_to_hoff(oid); 
  //off_t soff = hoff + ooff;
  ssize_t alloc_bytes = round_up(hoff + ooff + bytes) - hoff;
  off_t eoff = hoff + alloc_bytes; 
  off_t used_bytes = ooff + bytes;
  ssize_t limit_bytes = max_fbytes;


  ldout(cct, 10) << __func__ << " cid " << cid << " oid " << oid << " hoff " << hoff 
    << " alloc_bytes " << alloc_bytes << " max " << max_fbytes  << dendl;

  RWLock::WLocker l(lock);

  // check pent hoff 
  map<off_t, buddy_hindex_t>::iterator p, prev, next;
  p = hash_index_map.lower_bound(hoff);
  //만약에 여기서 hash_index_map 이 비어있으면?? 
  //뭐가 튀어나오는거지? 
  // prev 나 next 가 seg fault 날거 같은데 

  prev = p;
  next = p;
  if(p != hash_index_map.begin()) prev--;
  if(p != hash_index_map.end()) next++;

  // if (p == end?) // no bigger one
  // 	if (p == begin?) // no smaller one 
  //	   new_alloc
  // 	else
  // 	   lower_bound_check
  // 	   new_alloc
  //
  // if (it's me)
  // 	upper_bound_check
  // 	append or update
  //
  // else (found_hoff > hoff)
  // 	lower_bound_check
  // 	upper_bound_check
  // 	new_alloc
  
  ldout(cct, 10) << __func__  << " hash_index_map size " << hash_index_map.size() << " lowerbound " << p->first << dendl;
  _dump_hash_index_map(); 


  if (p == hash_index_map.end()) {
    if (p != hash_index_map.begin()) {
      // 이게 원소가 하나 있으면 어떻게 되나? 
      // begin 이랑 같은건가? 
      // end 인데 begin 은 아니다.. 
      // p 는 있지만 prev 는 없는거 아닌가? 
      // 근데 만약 
      // lower_bound_check
      ldout(cct, 10) << __func__ << " hash_index_map has elem" << dendl;
      if (prev->second.get_alloc_end() >= hoff) {
	ldout(cct, 10) << " cid " << cid << " case 1: prev " << prev->second << " hoff " << hoff << dendl;
	return -1;
	//goto collision;
      }
    }
    ldout(cct, 10) << __func__ << " empty has_index_map" << dendl;
    // upper limit 
    if(!(eoff < limit_bytes)){
      ldout(cct, 10) << " cid " << cid << "case 2: limit_bytes " << limit_bytes << " eoff " << eoff << dendl;
      return -1;
	//goto collision;
    }

    // new_alloc 
    buddy_hindex_t* nhindex = new buddy_hindex_t(oid, BD_DATA_T, hoff, bytes, round_up(bytes)); 
    auto result = hash_index_map.insert(make_pair(hoff, *nhindex));
    assert(result.second);
    ldout(cct, 10) << __func__ << " cid " << cid << " hash_map insert " << *nhindex << dendl;
  } 
  else {
    // it's me
    if (p->first == hoff) {

      //upper_bound_check 
      if (next != hash_index_map.end())
	limit_bytes = (++p)->second.hoff;

      if(!(eoff < limit_bytes)){
	ldout(cct, 10) << " cid " << cid << " case 3: limit_bytes " << limit_bytes << " eoff " << eoff << dendl;
	return -1;
      }
	//goto collision;
 
      // update index info 
      ldout(cct, 10) << __func__ << " cid " << cid << " hash_map update from " << p->second << dendl;
      p->second.alloc_bytes = alloc_bytes > p->second.alloc_bytes? alloc_bytes : p->second.alloc_bytes; 
      p->second.used_bytes = used_bytes > p->second.used_bytes? used_bytes : p->second.used_bytes;
      ldout(cct, 10) << __func__ << " cid " << cid << " hash_map update to " << p->second << dendl;
    } 

    // found bigger one 
    else {
      assert(p->first > hoff);

      if (p != hash_index_map.begin()) {
    	// lower_bound_check
      	if (prev->second.get_alloc_end() >= hoff){
	  ldout(cct, 10) << " cid " << cid << " case 4: prev " << prev->second << " hoff " << hoff << dendl;
	  return -1;
  	  //goto collision;
	}
      }

      //upper_bound_check 
      limit_bytes = p->first;
      
      if(!(eoff < limit_bytes)){
	ldout(cct, 10) << " cid " << cid << " case 5: limit_bytes " << limit_bytes << " eoff " << eoff << dendl;
	return -1;
	//goto collision;
      }

      // new_alloc 
      buddy_hindex_t* nhindex = new buddy_hindex_t(oid, BD_DATA_T, hoff, bytes, round_up(bytes)); 
      auto result = hash_index_map.insert(make_pair(hoff, *nhindex));
      assert(result.second);
      ldout(cct, 10) << __func__ << " cid " << cid << " hash_map insert " << *nhindex << dendl;
    }
  }


  // ooff 는 해당 object 에서 어디인지를 나타냄. 
  // 사실 write 를 할 때는 어차피 foff 랑 length 만 보고 쓰면 되니까 
  // ooff 는, 즉 이 데이터가 object 의 어디를 차지하는건지는.. 몰라도 되지. 
  // 게다가 ooff 는 0이 맞는데- collision 이 없어서 일단 하나로 쓰는 경우에는 0이 맞음.  
  //buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, ooff, hoff + ooff, bytes); 
  // 만약 그대로 ooff 를 쓰려면- 
  // write_data_bl 을 넘기지 말고- 원래 넘어온걸 넘기면.. 어떻게 되나?
  // 그런 경우에는 hoff + header_len 이 맞는데..  
  buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, 0, hoff + ooff, bytes); 
  iov.push_back(*niov);
  ldout(cct, 10) << __func__ << " niov " << *niov << " used_bytes " << used_bytes << dendl;

  return 0;
}

int BuddyHashIndexFile::release_space(const ghobject_t& oid)
{
  RWLock::WLocker l(lock);
  off_t hoff = hash_to_hoff(oid);
  return hash_index_map.erase(hoff);

  // 그리고 사실 punch hole 뚫어도 되는디.. 
}


/******
 * get_space_info
 */

int BuddyHashIndexFile::get_space_info(const ghobject_t& oid, const off_t ooff, const ssize_t bytes, 
    vector<buddy_iov_t>& iov)
{
  off_t hoff = hash_to_hoff(oid); 

  ldout(cct, 10) << __func__ << " cid " << cid << " oid " << oid << " hoff " << hoff << dendl;

  RWLock::RLocker l(lock);

  // check pent hoff 
  map<off_t, buddy_hindex_t>::iterator p = hash_index_map.find(hoff);

  if (p == hash_index_map.end()) {
    ldout(cct, 10) << "Failed to object file" << dendl;
    return -1;
  }

  // found - end check 
  if (p->second.used_bytes < ooff + bytes) {
    ldout(cct, 10) << "Error: data is smaller than requested" << dendl;
    return -1;
  }

  // found - normal  
  // ooff 는 read 에서는 사용할 필요 없음. 
  buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, 0, hoff + ooff, bytes); 
  iov.push_back(*niov);
  ldout(cct, 10) << __func__ << " niov " << *niov << dendl;

  return 0;
}



/******
 * truncate_space 
 */
int BuddyHashIndexFile::truncate_space(const ghobject_t& oid, ssize_t size)
{
  ldout(cct,10) << __func__  << " oid " << oid << " size " << size << dendl;

  off_t hoff = hash_to_hoff(oid);

  RWLock::WLocker l(lock);

  map<off_t, buddy_hindex_t>::iterator p = hash_index_map.find(hoff);

  if(p == hash_index_map.end())
    return -1; // no exist

  ldout(cct,10) << "before: oid " << oid << " ubytes " << p->second.used_bytes 
    << " abytes " << p->second.alloc_bytes << dendl;

  // add more space?
  ssize_t pad_bytes;
  off_t pad_off;
  if(size > p->second.used_bytes){
    pad_bytes = size - p->second.used_bytes;
    pad_off = p->second.used_bytes + 1;
    vector<buddy_iov_t> iov; 
    alloc_space(BD_DATA_T, oid, pad_off, pad_bytes, iov); 
  }
  else {
    p->second.used_bytes = size;
    p->second.alloc_bytes = round_up(p->second.used_bytes);
  }

  ldout(cct,10) << "after: oid " << oid << " ubytes " << p->second.used_bytes 
    << " abytes " << p->second.alloc_bytes << dendl;

  return 0;
}

/******
 * clone_space 
 */

int BuddyHashIndexFile::clone_space(const ghobject_t& ooid, 
    const ghobject_t& noid, vector<buddy_iov_t>& iov) 
{
  ldout(cct,10) << __func__  << " ooid " << ooid << " noid " << noid << dendl;

  RWLock::WLocker l(lock);
  // get ooid space 
  off_t ohoff = hash_to_hoff(ooid);

  map<off_t, buddy_hindex_t>::iterator op = hash_index_map.find(ohoff);
  if (op == hash_index_map.end()){
    ldout(cct,10) << "old oid is not found" << dendl;
    return -1;
  }
  return clone_space(ooid, noid, 0, op->second.used_bytes, 0, iov);
}

int BuddyHashIndexFile::clone_space(const ghobject_t& ooid, 
    const ghobject_t& noid, off_t srcoff, size_t bytes, off_t dstoff, 
    vector<buddy_iov_t>& iov)
{
  ldout(cct,10) << __func__  << " ooid " << ooid << " noid " << noid 
    << " srcoff " << srcoff << " bytes " << bytes << " dstoff " << dstoff<< dendl;

  RWLock::WLocker l(lock);

  // get ooid space 
  off_t ohoff = hash_to_hoff(ooid);
  off_t nhoff = hash_to_hoff(noid);
  ldout(cct,10) << "ohoff " << ohoff << " nhoff " << nhoff << dendl;

  map<off_t, buddy_hindex_t>::iterator op = hash_index_map.find(ohoff);
  if (op == hash_index_map.end()){
    ldout(cct,10) << "old oid is not found" << dendl;
    return -1;
  }

  // alloc space for new object 
  alloc_space(op->second.ctype, noid, 0, (ssize_t)(dstoff + bytes), iov);  
  
  ldout(cct,10) << "read_file " << fname << " " << ohoff+srcoff << "~" << bytes << dendl; 
  // read and copy 
  bufferlist src_bl;
  string err;
  src_bl.read_file(fname.c_str(), ohoff + srcoff, bytes, &err);

  ldout(cct,10) << "read size " << src_bl.length() << dendl;

  // write 
  // generate_iov()
  // buddy_iov_t* niov = new buddy_iov_t(BD_ORIG_F, fname, 0, nhoff + dstoff, bytes);
  // alloc_space create iovector in iov 
  for(vector<buddy_iov_t>::iterator ip = iov.begin();
      ip != iov.end(); ip++){
    assert(ip->data_bl.length() == 0);
    ip->data_bl.substr_of(src_bl, (*ip).ooff, (*ip).ooff + (*ip).bytes);
    (*ip).ooff = 0;
  }
  //niov->data_bl.claim(src_bl);

  return 0;

}


/******
 * write_fd 
 */
    
int BuddyHashIndexFile::write_fd(bufferlist& bl, uint64_t foff)
{
  ldout(cct, 10) << __func__ << " hfd " << hfd << " bl.length " << bl.length() << " foff " << foff << dendl;

  if (hfd < 0){
    ldout(cct, 10) << __func__ << " file is not open" << dendl;
    return 1;
  }


  // foff 도 align 맞춰줘야 함.. 젠장.. 
  // 앞의 부분 없으면 채워서 보내줘야 함... 
  // 여기서 보내기 전에 BLK_SIZE (적어도 512바이트 단위) 로 size 맞춰줘야 함.
  off_t orig_len = bl.length();
  off_t align_len = round_up (bl.length());
  off_t align_foff = round_down (foff);
  ldout(cct, 10) << __func__ << " align_len " << align_len << " align_off " << align_foff << dendl;

  bufferlist abl;

  // 원래 여기서 기존 데이터 읽어와야 하는데.. 우선 걍 하자. 
  abl.append_zero(foff - align_foff);
  abl.claim_append(bl);
  abl.append_zero(align_len - orig_len);

  if(directio)
    abl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);

  ldout(cct, 10) << __func__ << " hfd " << hfd << " abl.length(align) " << abl.length() << " align_foff " << align_foff << dendl;

  assert(abl.length() % BUDDY_FILE_BSIZE == 0);
  assert(align_foff % BUDDY_FILE_BSIZE == 0);

  int ret = abl.write_fd(hfd, align_foff);
    
  if (ret) {
//    cerr << __func__ <<  " write_fd error: "
//	 << cpp_strerror(ret) << std::endl;

    ldout(cct, 10) << __func__ <<  " Error in write "
	 << cpp_strerror(ret) << dendl;
//    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  return 0;
}


/******
 * sync 
 */
    
void BuddyHashIndexFile::sync()
{
  if (directio) {
    ldout(cct,10) << __func__ << " O_DSYNC " << dendl;
    return;
  }
  
  if (hfd < 0) {
    ldout(cct, 10) << __func__ << " file is not open" << dendl;
    return;
  }
    
  int ret = 0;
#if defined(DARWIN) || defined(__FreeBSD__)
  ret = ::fsync(hfd);
#else
  ret = ::fdatasync(hfd);
#endif
  if (ret < 0) {
    //derr << __func__ << " fsync/fdatasync failed: " << cpp_strerror(errno) << dendl;
    ldout(cct, 10) << __func__ << " fsync/fdata sync failed " << dendl;
    ceph_abort();
  }

  return;
}




/******
 * read_fd 
 */
int BuddyHashIndexFile::read_fd(bufferlist& bl, uint64_t foff, size_t len)
{

  // 사실은 iov 를 받아서 처리해야 하는게 맞음. 
  // 다 따로따로 떨어져 있을테니까..

  ldout(cct,10) << __func__  << " foff " << foff << " len " << len << dendl;
  
  if (directio)
    bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);

  int ret = bl.read_fd(hfd, foff, len);

  if (ret < static_cast<int>(len)) 
    ldout(cct, 10) << __func__ << "Error in read" << dendl;

  return ret;
}



/******
 * preallocate 
 */
int BuddyHashIndexFile::preallocate(uint64_t offset, size_t len)
{
  ldout(cct, 10) << __func__ << " fd " << hfd << " offset " << offset << " len " << len << dendl;

//#ifdef CEPH_HAVE_FALLOCATE
  if (hfd < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return -1;
  }

  int ret;

  // 파일 읽기. 
  // 만약 파일 끝을 preallocate 할거면 이렇게 해야함.  
#if 0
  struct stat st;
  ret = ::fstat(**fd, &st);
    
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return -errno;
  }
#endif
     
  ret = ftruncate(hfd, len);
  //ret = fallocate(**fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, offset, len);
    
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " Failed to preallocate" << dendl; 
    return -errno;
  } 

  ret = fallocate(hfd, 0, offset, (uint64_t)1 << 34); // 16G preallocate  
#if 0
  else {
      // ensure we extent file size, if needed
      if (offset + len > (uint64_t)st.st_size) {
	ret = ::ftruncate(**fd, offset + len);
	if (ret < 0) {
	  ret = -errno;
	  lfn_close(fd);
	  goto out;
	}
      }
    }
    lfn_close(fd);

    if (ret >= 0 && m_filestore_sloppy_crc) {
      int rc = backend->_crc_update_zero(**fd, offset, len);
      assert(rc >= 0);
    }

    if (ret == 0)
      goto out;  // yay!
    if (ret != -EOPNOTSUPP)
      goto out;  // some other error
//# endif
#endif
  return 0;
}

void BuddyHashIndexFile::dump_hash_index_map()
{

  RWLock::RLocker l(lock);

  ldout(cct, 10) << __func__ << " cid " << cid << " size " << hash_index_map.size() << dendl;
  int count = 0;
  for (map<off_t, buddy_hindex_t>::iterator p = hash_index_map.begin();
      p != hash_index_map.end(); p++)
  {
    ldout(cct, 10) << count << " : " << p->second << dendl; 

  }
}



void BuddyHashIndexFile::_dump_hash_index_map()
{

  ldout(cct, 10) << __func__ << " cid " << cid << " size " << hash_index_map.size() << dendl;
  int count = 0;
  for (map<off_t, buddy_hindex_t>::iterator p = hash_index_map.begin();
      p != hash_index_map.end(); p++)
  {
    ldout(cct, 10) << count << " : " << p->second << dendl; 

  }
}




