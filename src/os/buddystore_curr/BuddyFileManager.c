
/***********************


BuddyFileManager 

// maintain four files 
  int _super_fd;
  int _coll_fd;
  int _data_fd;
  int _bitmap_fd;


/// mkfs operations 

1. mount : _load
2. umount : _save

3. exists
4. list_collections
5. mkfs
6. read
7. statfs

8. _do_transactions 
OP_MKCOLL: _create_collection
OP_NOP: 
OP_OMAP_SETKEYS: _omap_setkeys
OP_TOUCH: _touch
OP_WRITE: _write



//////
0. mkfs
  current: 
  - create "collection" file 
  - store set <coll_t> collections into "collection" file 
  - 이렇게 해놔야 쓸수 있게 되는건가.. 안하면 없어지나. 
  - 뭐라도 써야하니까 빈 collections set 을 적어두는 듯. 

  Buddy:
  - 우리도 collections 를 관리하기 위한 string, o_offsetv 으로 superfile 을 초기화 시켜야함.
  - buddy.super 를 만들기. <string, o_offsetv> 로 bufferlist 에 encode 하고 저장하기. 


1. mount 
  - 

2. umount 

3. exists 

  bool exists(const coll_t& cid, const ghobject_t& oid) override;

  current:
  - check the object exists (not collection) 
  
  buddy: 
  - 만약 mestore 의 in-memory 자료구조를 그대로 쓸 경우엔 할 거 없음 
  - buddystore 를 완성하고 나면 일정크기까지만 caching 할거니까 miss 날 것임. 
  - 그렇더라도. collection 정보는 그대로 유지하도록 하면. 
  - collection 만 참고하면 되니까. 
  - collection 정보도 miss 나면 collection file 에 가서 읽어와야 함. 

4. list_collections 

  int list_collections(vector<coll_t>& ls) override;

  current:
  - push back all coll_t to list  
  
  buddystore: 
  - 3번과 같은 이유로 할  없음. 


5. mkfs 
  int mkfs() override;

  current: 
  - write "fs_fsid" as a meta file 
  - mkdir "collections" in the mount path

  buddy:
  - nothing 

6. read 

  int read(
    const coll_t& cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    uint32_t op_flags = 0,
    bool allow_eio = false) override;

  current: 
  - read object as a whole  
  - return data in bufferlist 

  buddy:
  - assume no caching  
  - load meta 
  - look-up coll_map in meta -> get info <cid, offsetv> 
  - load collection data block 
  - look-up object map in a collection block -> get info <oid, offsetv> 
  - load object data block  
  - return 

  create collection "meta" 
  create superbock object 

//// 

7. statfs 
 
  int statfs(struct store_statfs_t *buf) override;

  current: 
  - fill out store_statfs_t data 
  - e.g., total capacity, available capacity, etc. 

	struct store_statfs_t
	{
	  uint64_t total = 0;                  // Total bytes
	  uint64_t available = 0;              // Free bytes available
	
	  int64_t allocated = 0;               // Bytes allocated by the store
	  int64_t stored = 0;                  // Bytes actually stored by the user
	  int64_t compressed = 0;              // Bytes stored after compression
	  int64_t compressed_allocated = 0;    // Bytes allocated for compressed data
	  int64_t compressed_original = 0;     // Bytes that were successfully compressed
	
	};

  buddy:
  - buddy system 은 파일을 가지고 store 를 관리함. 
  - 이와 관련된 자료구조를 관리하는 BuddyFileManager 가 필요할듯. 
  - 현재 memstore 파일을 고치지 말고 BuddyFileManager 를 만들어서 storage 와 관련된 것은 그쪽에서 처리하도록 하자. 
  - 그리고 연산 발생 시 필요한 경우 그쪽의 API를 부르기로. 
  - 대부분의 연산은 거의 필요할듯. 
  - alloc 같은 건 내부적으로. read, write 등은 밖으로 보여주기. 
  - collection 을 만든다면.. collection file 에 써야할듯. 
  - API 는 그냥 create_collection 정도로 보여주고. 


8. _do_transactions 

OP_MKCOLL: _create_collection

  int _create_collection(const coll_t& c, int bits);
  current:
  - insert a pair of coll_t and struct collection into coll_map 

  buddy:
  - insert s_coll_map 
  - write meta file 
  - 근데 파일은.. meta 파일을 업데이트 하거나, collection object 를 collection file 에 쓰는건데.. 
  - 두개다 안하면. 복구 안됨. meta 파일을 업데이트 하려면.. map tree 를 적어야 함. 이게 오히려 성능에 더 나쁠수 있는데, 일단은 그냥.. meta file 업데이트 하는걸로 구현해보자. 
  - 그런데 c++ 에서 map 클래스는 red-black tree 로 구현된걸로 아는데.. 이걸 전체 또 다 쓰려면. 오버헤드 클 듯.. 
  - 나중에 스토리지에 적합한 자료구조로 만들어야 할듯. 

  - assume no caching  
  - load meta 
  - look-up coll_map in meta -> get info <cid, offsetv> 
  - load collection data block 
  - look-up object map in a collection block -> get info <oid, offsetv> 
  - load object data block  
  - return 

OP_NOP: 
  current: nothing 
 
OP_OMAP_SETKEYS: _omap_setkeys

  int BuddyStore::_omap_setkeys(const coll_t& cid, const ghobject_t &oid, bufferlist& aset_bl)

  current:
  - decode <key, value> from the buffer list  
  - insert <key, value> into omap <string, bufferlist> in the object 

  buddy:
  - write offset object omap 
  - 일단은 omap 을 현재 bufferlist 에 있는거 다 내려보자. 막 업데이트 자주 일어나거나 데이터가 크지 않아서 4KB 안에 있다면 파일에 함께 적는게 나쁘지않을 것임. 
  - arg 로 넘어온 buffer list 가 omap 전체가 아니고 새로 추가하려는 <k,v> 만 가지고 있을듯. 

  이게.. 구현할때는 align 도 맞춰야 하고 블록단위 I/O를 해야하니까.. in-memory 에 해당 데이터의 buffer 를 캐슁하고 있다고 생각하고 자료구조를 만들어야 할듯. 이게 서로 변환이 잘 안되면.. 엄청 피곤할 수 있음. 

  - 그리고 kv 는 object id 와 함께 leveldb 같은거 그냥 써버리는게 나을지도 모름. 
  - _do_transaction 에 있는걸 다 처리한 후에 한번에 sync 날리면어떤가? 
  - 그니까 써야할 데이터들을.. 묶어서 buffer list 에 담아두고, do_transaction 이 종료돌 떄 한꺼번에 쓰는것임. checksum 이 필요한건 알아서 써질테고. ==> batching 을 하자는 뜻임. 
  - 

OP_TOUCH: _touch

  int _touch(const coll_t& cid, const ghobject_t& oid);

  current:
  - get or create an object 
  - update object_hash and object_map

  buddy:
  - write 가 아니고 touch 면 고민되네. object creation 인데..  
  - file store 에서는? directory 까지 모두 log 로 묶어서 처리하니까 문제 없음. 
  - 우선은 sync 안함. 빈 파일은 버리고. data 가 있는 파일은 살림. 실제 데이터가 해당 파일에 적히면 복구 가능함. 


OP_WRITE: _write

  int _write(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl, uint32_t fadvise_flags = 0);

  current:
  - get an object in the given collection
  - write new data in appropriate location

  buddy:
  - send the buffer to storage (data only)
  - 여기서는 data 만 보내는데- 이거 다 묶어서.. 만약 setkeys 같은게 같은 object 애 들어왔다면 묶어서 보내도록 해야할듯. 
  - write full 이면 checksum 도 보내야 할것이고, partial write 면 shadow 에 보내야 함. 
  - offset 을 주고 write 하는거 보기. ==> 여기 있는듯. Buffer 클래스 인터페이스에도 있고. 


현재는 I/O를 다 bufferlist 통해서 수행함. 
encode(data, bl); // data 를 bufferlist 에 적음. 
bl.write_fd(); // 해당 buffer 를 주어진 파일에 적음. 

만약 모든 I/O 를 묶어서 하려면 bufferlist 를 따로 두어야 함. 그리고 각각 write 를 보내야함. 
현재 pending 되어 있는 op 들에 대해서 연속된 것을 bufferlist 로 합치면 됨. 
즉, object 에 write 가 오고 바로 xattr 가 오는 경우. 그럼 합쳐서 매달아 놓음. checksum 은 실제 I/O 보내기 직전에 붙일 것. 

=====
1차 버전에서는 



// logging code 보기 


















