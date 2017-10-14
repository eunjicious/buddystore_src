#include "BuddyLogDataFileObject.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "buddyfilestore " 

const static int64_t ONE_MEG(1 << 20);
const static int CEPH_DIRECTIO_ALIGNMENT(4096);
/**********************************
 *  BuddyHashIndexFile functions 
 **********************************/


BuddyLogDataFileObject::BuddyLogDataFileObject(CephContext* cct_, 
      string fname_, bool dio=true, bool prealloc=true) :
      cct(cct_),
      fname(fname_),
      directio(dio),
      dfd(-1),
      tail_off(0),
      total_used_bytes(0),
      total_alloc_bytes(0),
      total_hole_bytes(0),
      prealloc_bytes(0),
      prewrite_unit_bytes(cct->_conf->buddystore_file_prewrite_unit),
      file_prewrite(cct->_conf->buddystore_file_prewrite),
      file_prealloc(!file_prewrite),
      file_inplace_write(cct->_conf->buddystore_file_inplace_write),
      lock("BuddyLogDataFileObject::lock") {

	//create_or_open_file(0);
    
      } 


/******
 * create_or_open_file  
 */
int BuddyLogDataFileObject::create_or_open_file(int out_flags)
{
  ldout(cct,10) << __func__ << dendl;

  int flags = O_RDWR | O_CREAT;
  flags |= out_flags;
    
  if (directio) 
    flags |= O_DIRECT | O_DSYNC;

  // open 
  int r = ::open(fname.c_str(), flags, 0644);
  if (r < 0){
    ldout(cct,10) << __func__ << "Failed to create file: " << fname << cpp_strerror(r) << dendl; 
    return r;
  }
  dfd = r;

  struct stat st;
  r = ::fstat(dfd, &st);
    
  if (r < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return r;
  }
  ldout(cct, 10) << __func__ << " data_file size " << st.st_size << dendl;


  // we can hold it for later .. 
  if(file_prealloc){
    ldout(cct, 10) << __func__ << " file_prealloc is set " << dendl;
    preallocate(0, BUDDY_PREALLOC_SIZE);
  }

  // prewrite ... 
  if (file_prewrite) {

    ldout(cct, 10) << __func__ << " prewrite_unit_bytes " << prewrite_unit_bytes << dendl;
    bufferlist bl;
    bl.append_zero(prewrite_unit_bytes);

    off_t prewrite_size = prewrite_unit_bytes * 1024;
    off_t pos = st.st_size;

    ldout(cct,10) << __func__ << " current data_file size " << pos << dendl;

    while (pos < prewrite_size) {
      r = write_fd(bl, pos);
      //pos += bl.length();
      pos += prewrite_unit_bytes;
    }

    ldout(cct, 10) << __func__ << " prewrite write ret = " << r << dendl;
  }

  r = ::fstat(dfd, &st);

  ldout(cct,10) << __func__ << " data_file_size " << st.st_size << dendl;

  //::close(fd);
  return 0;
}

/******
 * delete_file  
 */
int BuddyLogDataFileObject::delete_file()
{
  ldout(cct,10) << __func__ << dendl;

  if (dfd > 0)
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
int BuddyLogDataFileObject::close_file()
{
  if (dfd < 0){
    ldout(cct, 10) << "no file exists" << dendl; 
    return -1;
  }

  //int r = ::close(fd);

  VOID_TEMP_FAILURE_RETRY(::close(dfd));
  return 0; 

}

/******
 * stat_file
 */
void BuddyLogDataFileObject::stat_file()
{
  ldout(cct, 10) << " fname " << fname << 
	" tail_off " << tail_off <<  
	" total_used_bytes " << total_used_bytes << 
	" total_alloc_bytes " << total_alloc_bytes << dendl;
}

/*****************
 * index update
 ****/
// move to collection ... 
#if 0
int BuddyLogDataFileObject::insert_index(const ghobject_t& oid, const off_t ooff, 
    const off_t foff, const ssize_t bytes )
{


  ldout(cct, 10) << __func__ << " oid " << oid << " ooff " << ooff <<
    " foff " << foff << " bytes " << bytes << dendl;

  RWLock::WLocker l(lock);

  buddy_index_map_t* omap;
  buddy_index_t* nidx = new buddy_index_t(ooff, foff, bytes); 

  off_t ns = 0, ne = 0, os = 0, oe = 0;
  map<off_t, buddy_index_t>::iterator sp, p;

  // find map 
  map<ghobject_t, buddy_index_map_t>::iterator omap_p = log_index_map.find(oid);

  // not found? create and insert entry. done! 
  if(omap_p == log_index_map.end()){
    ldout(cct, 15) << __func__ <<  " oid is not found. create index map " << dendl;

    // create index_map 
    omap = new buddy_index_map_t();
    log_index_map.insert(make_pair(oid, (*omap)));
    omap_p = log_index_map.find(oid);
  }

  // found map 
  omap = &omap_p->second;
  sp = omap->index_map.upper_bound(ooff);
  
  if (sp == omap->index_map.begin() && sp == omap->index_map.end()){
    ldout(cct, 15) << __func__ << " empty index map " << dendl;
    goto insert_new_index;
  }

  // prev 부터 돌면서 남아있는게 없을 때까지 punch out 
  // 겹치는게 있으면 original map 에서 줄여버리거나 삭제. 
  
  ns = ooff;
  ne = ooff + bytes;

  if(sp!= omap->index_map.begin()) p = --sp;
  else p = sp;

  /// punch out 
  // 내가 짰지만.. 정말 잘짠거 같구나.. 
  while(p != omap->index_map.end()){
    os = p->second.ooff;
    oe = p->second.ooff + p->second.used_bytes;

    if (os > ne) 
      break;

    if (oe < ns)
      continue;


    buddy_index_t prev_idx = p->second;
    buddy_index_t post_idx = p->second;

    omap->index_map.erase(os);

    if(ns == os && ne == oe) {
      break;
    }

    if(ns > os && ns < oe){
      prev_idx.used_bytes -= (oe -ns + 1);
      omap->index_map.insert(make_pair(prev_idx.ooff, prev_idx));
    }

    if(ne > os && ne < oe) {
      post_idx.ooff -= (ne - os + 1);
      post_idx.foff -= (ne - os + 1);
      post_idx.used_bytes -= (ne - os + 1); 
      omap->index_map.insert(make_pair(post_idx.ooff, post_idx));
    }
    p++;
  }

  insert_new_index:
  // insert 
  omap->index_map.insert(make_pair(ooff, (*nidx)));

// for debugging... 
  for(map<off_t, buddy_index_t>::iterator tmp = omap->index_map.begin(); 
      tmp != omap->index_map.end() ; tmp++){
    ldout(cct, 10) << __func__ << " oid " << oid << " index_map = " << (*tmp) << dendl;
  }
  
  return 0;

}
#endif

/******
 * alloc_space 
 * 중요 함수 
 */

int BuddyLogDataFileObject::alloc_space(coll_t cid, const ghobject_t& oid, 
    const off_t ooff, const ssize_t bytes, vector<buddy_iov_t>& iov)
{
  Mutex::Locker l(lock);

  ldout(cct, 10) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << dendl;

  off_t eoff = ooff + bytes - 1;
  off_t bsoff = round_down(ooff); 
  off_t beoff = round_up(eoff);
  off_t off_in_blk = ooff - bsoff;
  uint64_t alloc_bytes = beoff - bsoff;

  // 1. allocate space at block granularity 
  off_t foff = tail_off + off_in_blk;
  tail_off += alloc_bytes;
  total_alloc_bytes += alloc_bytes;
  total_used_bytes += bytes;


//  if( file_prealloc && ((prealloc_bytes - total_alloc_bytes) < BUDDY_PREALLOC_LOW_WATERMARK)) 
//    preallocate(prealloc_bytes, BUDDY_PREALLOC_SIZE);

 
  // 3. create buddy_iov_t  
  //buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, ooff, foff, bytes); 
  //
  // 여기에서 0 대신에 ooff 를 주면 문제가 생김. 
  // 넘어온 data_bl 에서 "어디서부터 copy" 해올지를 저거 보고 결정하는데 
  // 0 을 주면 "지금 넘어온거의 처음" 부터라는 의미. 
  // 즉, off_in_src 가 되는게 맞음. 
  // 만약 쪼개진다면- 저걸 그 offset 에 맞게 0대신 세팅해야함. 
  // log 에서 하나로 할당할 때는 저렇게 하니까. 
  //
  //buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, 0, foff, bytes); 
  buddy_iov_t* niov = new buddy_iov_t(cid, oid, fname, 0, ooff, foff, bytes, alloc_bytes); 
  assert(niov->off_in_blk == off_in_blk);

  iov.push_back(*niov);

  ldout(cct, 10) << __func__ << " niov " << *niov << " tail_off = " << tail_off << 
    " alloc_bytes " << alloc_bytes << " total_used_bytes " << total_used_bytes << dendl;    

#if 0
  // 이 부분은.. write 에서 해야함 
  // 2. insert_index 로 추가시키기. 
  //insert_index(niov);
  int r = insert_index(oid, ooff, foff, bytes);
  if(r < 0)
    ldout(cct, 10) << __func__ << " Failed to insert index " << dendl;
#endif
 
  return 0;
}

/**********
 * release_space
 * */

//int BuddyLogDataFileObject::release_space(const ghobject_t& oid)
int BuddyLogDataFileObject::release_space(const buddy_index_map_t& omap)
{

  Mutex::Locker l(lock);

  // 해당 object 의 map을 받아서 지우고 return 하면 
  // 호출한 함수에서 마저 지우면 됨.. 

#if 0
  // 여기는 _remove 로 이동하고 
  // find map 
  map<ghobject_t, buddy_index_map_t>::iterator omap_p = log_index_map.find(oid);

  // not found? create and insert entry. done! 
  if(omap_p == log_index_map.end()){
    ldout(cct, 15) << __func__ <<  " oid index does not exist " << dendl;
    return 0;
  }
#endif

  // found map 
  //buddy_index_map_t omap = omap_p->second;

  for(map<off_t, buddy_index_t>::const_iterator p = omap.index_map.begin();
      p != omap.index_map.end(); p++) {

    ldout(cct, 10) << __func__ << " free index : " << p->second << dendl;
    // add to free map
    auto r = free_index_map.insert(make_pair(p->second.foff, p->second.alloc_bytes));
    if(!r.second) {
      ldout(cct, 10) << __func__ << " free_index_map already contains buddy_index_t " << *p << dendl;
      assert(0);
    }

    // punch_hole : we might want to coalescing holes in the future..
    int ret = fallocate(dfd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, p->second.foff, p->second.alloc_bytes);

    if (ret < 0 ){
      ldout(cct, 10) << __func__ << " failed to punch_hole " << *p << dendl;
      assert(0);
    }
  }

  ldout(cct, 10) << __func__ << " free index size : " << free_index_map.size() << dendl;


  // 이 부분은.. _remove 로 이동. 
  //auto r = log_index_map.erase(oid);

  return 0;
}

#if 0
/******
 * get_space_info
 */

int BuddyLogDataFileObject::get_space_info(const ghobject_t& oid, const off_t ooff, const ssize_t bytes, 
    vector<buddy_iov_t>& iov)
{


  ldout(cct, 10) << __func__ << " cid " << cid << " oid " << oid << " ooff " << ooff << " bytes " << bytes << dendl;

  RWLock::RLocker l(lock);

  // find map 
  map<ghobject_t, buddy_index_map_t>::iterator omap_p = log_index_map.find(oid);

  if(omap_p == log_index_map.end()){
    return -1;
  }

  // found map 
  buddy_index_map_t* omap = &omap_p->second;
  map<off_t, buddy_index_t>::iterator p;

// for debugging... 
  for(map<off_t, buddy_index_t>::iterator tmp = omap->index_map.begin(); 
      tmp != omap->index_map.end() ; tmp++){
    ldout(cct, 10) << __func__ << " oid " << oid << " index_map = " << (*tmp) << dendl;
  }


  ssize_t rbytes = bytes;
  off_t soff = ooff;
  off_t eoff = ooff + bytes;
  off_t foff = 0;
  ssize_t fbytes = 0;
  off_t peoff = 0;

  p = omap->index_map.upper_bound(soff);
  if (p == omap->index_map.begin()){
    ldout(cct, 10) << __func__ << " not found index starting with ooff " << ooff << dendl;
    return -1;
  }

  p--;

  while(rbytes > 0) {

    ldout(cct, 20) << __func__ << " rbytes = " << rbytes << " p.ooff " << p->second.ooff << " p.eoff " << p->second.ooff + p->second.used_bytes << dendl;

    assert(soff >= p->second.ooff && soff <= (p->second.ooff + p->second.used_bytes));

    peoff = p->second.ooff + p->second.used_bytes;

    foff = p->second.foff + (soff - p->second.ooff);
    fbytes = (peoff < eoff? peoff : eoff) - soff;

    buddy_iov_t* niov = new buddy_iov_t(&cid, BD_ORIG_F, fname, soff, foff, fbytes); 
    iov.push_back(*niov);

    rbytes -= fbytes;
    soff += fbytes;

    p++;
  }

  assert(rbytes == 0);


  return 0;
}
#endif


#if 0
/******
 * truncate_space 
 */
int BuddyLogDataFileObject::truncate_space(const ghobject_t& oid, ssize_t size)
{
  ldout(cct,10) << __func__  << " oid " << oid << " size " << size << dendl;

  off_t hoff = hash_to_hoff(oid);

  RWLock::WLocker l(lock);

  map<off_t, buddy_lindex_t>::iterator p = hash_index_map.find(hoff);

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

int BuddyLogDataFileObject::clone_space(const ghobject_t& ooid, 
    const ghobject_t& noid, vector<buddy_iov_t>& iov) 
{
  ldout(cct,10) << __func__  << " ooid " << ooid << " noid " << noid << dendl;

  RWLock::WLocker l(lock);
  // get ooid space 
  off_t ohoff = hash_to_hoff(ooid);

  map<off_t, buddy_lindex_t>::iterator op = hash_index_map.find(ohoff);
  if (op == hash_index_map.end()){
    ldout(cct,10) << "old oid is not found" << dendl;
    return -1;
  }
  return clone_space(ooid, noid, 0, op->second.used_bytes, 0, iov);
}

int BuddyLogDataFileObject::clone_space(const ghobject_t& ooid, 
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

  map<off_t, buddy_lindex_t>::iterator op = hash_index_map.find(ohoff);
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

#endif

/******
 * write_fd 
 */
    
int BuddyLogDataFileObject::write_fd(bufferlist& bl, uint64_t foff)
{
  ldout(cct, 5) << __func__ << " dfd " << dfd << " bl.length " << bl.length() << " foff " << foff << dendl;

  if (dfd < 0){
    ldout(cct, 10) << __func__ << " file is not open" << dendl;
    return 1;
  }

/**** alignment has to be made in buddystore 

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
*/

  if(directio){
    assert((foff % BUDDY_FALLOC_SIZE) == 0);
    assert((bl.length() % BUDDY_FALLOC_SIZE) == 0);
    bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);
  }

#if 0
  ldout(cct, 10) << __func__ << " dfd " << dfd << " abl.length(align) " << abl.length() << " align_foff " << align_foff << dendl;

  assert(abl.length() % BUDDY_FALLOC_SIZE == 0);
  assert(align_foff % BUDDY_FALLOC_SIZE == 0);
#endif

  utime_t start = ceph_clock_now(); 

  int ret = bl.write_fd(dfd, foff);
    
  if (ret) {
    ldout(cct, 10) << __func__ <<  " Error in write "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }

  utime_t lat = ceph_clock_now();
  lat -= start;
  
  ldout(cct, 5) << __func__ << " data_file_write_fd lat " << lat << dendl;

  return 0;
}


/******
 * sync 
 */
    
void BuddyLogDataFileObject::sync()
{
  if (directio) {
    ldout(cct,10) << __func__ << " O_DSYNC " << dendl;
    return;
  }
  
  if (dfd < 0) {
    ldout(cct, 10) << __func__ << " file is not open" << dendl;
    return;
  }
    
  int ret = 0;
#if defined(DARWIN) || defined(__FreeBSD__)
  ret = ::fsync(dfd);
#else
  ret = ::fdatasync(dfd);
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
int BuddyLogDataFileObject::read_fd(bufferlist& bl, uint64_t foff, size_t len)
{
  // 사실은 iov 를 받아서 처리해야 하는게 맞음. 
  // 다 따로따로 떨어져 있을테니까..

  ldout(cct,10) << __func__  << " foff " << foff << " len " << len << dendl;
  
  if (directio)
    bl.rebuild_aligned(CEPH_DIRECTIO_ALIGNMENT);


  ldout(cct,10) << __func__ << " read_debug: dfd " << dfd << " foff " << foff << " len " << len << " bl.length() " << bl.length() << dendl;

  // for test 
  ssize_t blen = round_up(len);
  ssize_t boff = round_down(foff);
  bufferlist bbl;

  ldout(cct,10) << __func__ << " read_debug: boff " << boff << " blen " << blen << dendl;

  int ret = bbl.read_fd(dfd, boff, blen);
  ldout(cct,10) << __func__ << " read_debug: direct read: ret " << ret << dendl; 

  ldout(cct,10) << __func__ << " read_debug: bbl.length() " <<  bbl.length() << " substr off " << foff - boff << 
    " len " << len << dendl; 
  bl.substr_of(bbl, foff - boff, len);
  ret = bl.length();

  //int ret = bl.read_fd(dfd, foff, len);
  
  if (ret < static_cast<int>(len)) 
    ldout(cct, 10) << __func__ << "Error in read: " << cpp_strerror(ret) << dendl;

  return ret;
}



/******
 * preallocate 
 */
int BuddyLogDataFileObject::preallocate(uint64_t offset, size_t len)
{
  ldout(cct, 10) << __func__ << " fd " << dfd << " offset " << offset << " len " << len << dendl;

//#ifdef CEPH_HAVE_FALLOCATE
  if (dfd < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return -1;
  }


//#if 0  // 이부분이 아무것도 아닌거 같은데 성능에 영향이 크듯. 
  int ret;

  // 파일 읽기. 
  // 만약 파일 끝을 preallocate 할거면 이렇게 해야함.  
  struct stat st;
  ret = ::fstat(dfd, &st);
    
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " File is not open" << dendl; 
    return -errno;
  }


  // read file size 
  off_t fsize = st.st_size;
  off_t soff = round_up(fsize - 1);

  soff = soff < offset ? offset : soff;
  assert(soff % BUDDY_FALLOC_SIZE == 0);

  size_t alloc_bytes = 0;
  size_t alloc_unit = 1 << 20; // 1MB 
  alloc_unit = len < alloc_unit? len : alloc_unit;
    
  ret = ftruncate(dfd, len);
  //ret = fallocate(**fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, offset, len);
    
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " Failed to ftruncate" << dendl; 
    return -errno;
  } 

  ret = fallocate(dfd, 0, offset, len);
  if (ret < 0) {
    ldout(cct, 10) << __func__ << " Failed to fallocate" << dendl; 
    return -errno;
  }

  ldout(cct, 10) << __func__ << " fd " << dfd << " offset " << offset << " len " << len << " succeed " << dendl;
  //ret = fallocate(dfd, 0, offset, (uint64_t)1 << 34); // 16G preallocate  
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
//#endif // 여기 
  return 0;
}


