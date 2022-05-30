#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  char* buffer = buf;
  MACH_WRITE_TO(uint32_t, buffer, CATALOG_METADATA_MAGIC_NUM);
  buffer+=sizeof(uint32_t);
  uint32_t len = table_meta_pages_.size();
  for(auto it:table_meta_pages_){
    if(it.second == INVALID_PAGE_ID) len--;
  }
  // cout<<len<<endl;
  MACH_WRITE_TO(uint32_t, buffer, len);
  buffer+=sizeof(uint32_t);
  auto it1 = table_meta_pages_.begin();
  for(int i=0;i<int(table_meta_pages_.size());i++){
    uint32_t tid = it1->first;
    int32_t pid = it1->second;
    if(pid<0) continue;
    MACH_WRITE_TO(uint32_t, buffer, tid);
    buffer+=sizeof(uint32_t);
    MACH_WRITE_TO(int32_t, buffer, pid);
    buffer+=sizeof(int32_t);
    it1++;
  }
  len = index_meta_pages_.size();
  for(auto it:index_meta_pages_){
    if(it.second == INVALID_PAGE_ID) len--;
  }
  // cout<<len<<endl;
  MACH_WRITE_TO(uint32_t, buffer, len);
  buffer+=sizeof(uint32_t);
  auto it2 = index_meta_pages_.begin();
  for(int i=0;i<int(index_meta_pages_.size());i++){
    uint32_t iid = it2->first;
    int32_t pid = it2->second;
    if(pid<0) continue;
    MACH_WRITE_TO(uint32_t, buffer, iid);
    buffer+=sizeof(uint32_t);
    MACH_WRITE_TO(int32_t, buffer, pid);
    buffer+=sizeof(int32_t);
    it2++;
  }
  // ASSERT(false, "Not Implemented yet");
  // cout<<buffer-buf<<endl;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  char* buffer = buf;
  CatalogMeta* ret = new(heap->Allocate(sizeof(CatalogMeta)))CatalogMeta;
  [[maybe_unused]]uint32_t val = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  // cout<<val<<endl;//输出魔数
  uint32_t len = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  for(int i=0;i<int(len);i++){
    uint32_t tid = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
    int32_t pid = MACH_READ_FROM(int32_t, buffer); buffer+=4;
    ret->table_meta_pages_[tid] = pid;
  }
  ret->GetTableMetaPages()->emplace(len, INVALID_PAGE_ID);
  len = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  for(int i=0;i<int(len);i++){
    uint32_t iid = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
    int32_t pid = MACH_READ_FROM(int32_t, buffer); buffer+=4;
    ret->index_meta_pages_[iid] = pid;
  }
  ret->GetIndexMetaPages()->emplace(len, INVALID_PAGE_ID);
  return ret;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t len = table_meta_pages_.size();
  for(auto it:table_meta_pages_){
    if(it.second == INVALID_PAGE_ID) len--;
  }
  len += index_meta_pages_.size();
  for(auto it:index_meta_pages_){
    if(it.second == INVALID_PAGE_ID) len--;
  }
  len = len*8+4+8;
  return len;
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");
  if(init == true){
    catalog_meta_ = new(heap_->Allocate(sizeof(CatalogMeta)))CatalogMeta;
  }
  if(init == false) 
  {
    Page* p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char* t = p->GetData();
    catalog_meta_ = CatalogMeta::DeserializeFrom(t,heap_);
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
      
    for(auto it:catalog_meta_->table_meta_pages_){
      if(it.second<0) continue;
      p = buffer_pool_manager->FetchPage(it.second);
      t = p->GetData();
      TableMetadata* meta;
      meta->DeserializeFrom(t,meta, heap_);
      TableInfo* tinfo;
      tinfo = TableInfo::Create(heap_);
      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,meta->GetSchema(), nullptr, log_manager_, lock_manager_, heap_);
      tinfo->Init(meta, table_heap);
      table_names_[meta->GetTableName()] = meta->GetTableId();
      tables_[meta->GetTableId()] = tinfo;
      index_names_.insert({meta->GetTableName(), std::unordered_map<std::string, index_id_t>()});
    }
    for(auto it:catalog_meta_->index_meta_pages_){
      if(it.second<0) continue;
      p = buffer_pool_manager->FetchPage(it.second);
      t = p->GetData();
      IndexMetadata* meta;
      IndexMetadata::DeserializeFrom(t, meta, heap_);
      TableInfo* tinfo = tables_[meta->GetTableId()];
      IndexInfo* index_info = IndexInfo::Create(heap_);
      index_info->Init(meta,tinfo,buffer_pool_manager_);//segmentation
      index_names_[tinfo->GetTableName()][meta->GetIndexName()] = meta->GetIndexId();
      indexes_[meta->GetIndexId()] = index_info;
    }

    // Page* p2 = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
    // char* t2 = p2->GetData();
    // // cout<<"fetch page done"<<endl;
    // while(*t2!='\0'){
    //   IndexMetadata* meta;
    //   t2+=IndexMetadata::DeserializeFrom(t2, meta, heap_);
    //   TableInfo* tinfo = tables_[meta->GetTableId()];
    //   // cout<<meta->GetTableId()<<" "<<meta->GetIndexName()<<endl;
    //   // cout<<tinfo->GetTableName()<<" "<<tinfo->GetTableId()<<endl;
    //   IndexInfo* indinfo = IndexInfo::Create(heap_);
    //   indinfo->Init(meta,tinfo,buffer_pool_manager_);//segmentation
    //   // cout<<"p1"<<endl;
    //   index_names_[tinfo->GetTableName()][meta->GetIndexName()] = meta->GetIndexId();
    //   indexes_[meta->GetIndexId()] = indinfo;
    // }
    // buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
    // buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
    if(table_names_.count(table_name) != 0)
      return DB_TABLE_ALREADY_EXIST;

    table_id_t table_id = next_table_id_++;
    page_id_t page_id;//分配tableMetaData的数据页
    Page* new_table_page = buffer_pool_manager_->NewPage(page_id);
    catalog_meta_->table_meta_pages_[table_id] = page_id;
    catalog_meta_->table_meta_pages_[next_table_id_] = -1;
    TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, page_id, schema, heap_);
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,schema, txn, log_manager_, lock_manager_, heap_);
    table_info = TableInfo::Create(heap_);
    table_info->Init(table_meta, table_heap);


    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;
    index_names_.insert({table_name, std::unordered_map<std::string, index_id_t>()});
    ///将tablematedata写入数据页
    auto len = table_meta->GetSerializedSize();
    char meta[len+1];
    table_meta->SerializeTo(meta);
    char* p = new_table_page->GetData();
    memcpy(p,meta,len);

    len = catalog_meta_->GetSerializedSize();
    char cmeta[len+1];
    catalog_meta_->SerializeTo(cmeta);
    p = buffer_pool_manager_->FetchPage(0)->GetData();
    memset(p, 0, PAGE_SIZE);
    memcpy(p,cmeta,len);
    cout<<"CreatTable Success, <tid, pid> = "<<table_id<<", "<<page_id<<endl;
    if(table_meta!=nullptr && table_heap!=nullptr)
      return DB_SUCCESS;
    return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.count(table_name) <= 0)
    return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  for(auto it=tables_.begin();it!=tables_.end();it++){
    tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  // ASSERT(false, "Not Implemented yet");
    if(index_names_.count(table_name)<=0) return DB_TABLE_NOT_EXIST;
    auto it = index_names_[table_name];
    if(it.count(index_name)>0) return DB_INDEX_ALREADY_EXIST;

    index_id_t index_id = next_index_id_++;
    std::vector<uint32_t> key_map;
    TableInfo* tinfo;
    dberr_t error = GetTable(table_name,tinfo);
    if(error) return error;
    for(int j=0;j<int(index_keys.size());j++){
      auto i = index_keys[j];
      uint32_t tkey;
      dberr_t err = tinfo->GetSchema()->GetColumnIndex(i, tkey);
      if(err) return err;
      key_map.push_back(tkey);
    }
    IndexMetadata *index_meta_data_ptr = IndexMetadata::Create(index_id, index_name, table_names_[table_name],key_map,heap_ );
    
    index_info = IndexInfo::Create(heap_);
    index_info->Init(index_meta_data_ptr,tinfo,buffer_pool_manager_);
    
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;
    
    //将index的信息写入
    page_id_t page_id;//分配indexMetaData的数据页
    Page* new_index_page = buffer_pool_manager_->NewPage(page_id);
    catalog_meta_->index_meta_pages_[index_id] = page_id;
    catalog_meta_->index_meta_pages_[next_index_id_] = -1;
    
    auto len = index_meta_data_ptr->GetSerializedSize();
    char meta[len+1];
    index_meta_data_ptr->SerializeTo(meta);
    char* p = new_index_page->GetData();
    memcpy(p,meta,len);

    len = catalog_meta_->GetSerializedSize();
    char cmeta[len+1];
    catalog_meta_->SerializeTo(cmeta);
    p = buffer_pool_manager_->FetchPage(0)->GetData();
    memset(p, 0, PAGE_SIZE);
    memcpy(p,cmeta,len);
    cout<<"CreatIndex Success, <tid, pid> = "<<index_id<<", "<<page_id<<endl;

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto search_table = index_names_.find(table_name);
  if (search_table == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto index_table = search_table->second;
  auto search_index_id = index_table.find(index_name);
  if (search_index_id == index_table.end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = search_index_id->second;
  auto it = indexes_.find(index_id);
  if(it == indexes_.end()) return DB_INDEX_NOT_FOUND;
  index_info = it->second;
  return DB_SUCCESS;
                                 }
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  auto table_indexes = index_names_.find(table_name);
  if(table_indexes == index_names_.end())
    return DB_TABLE_NOT_EXIST;
  auto indexes_map = table_indexes->second;
  for(auto it:indexes_map){
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  
  auto it = table_names_.find(table_name);
  if(it == table_names_.end()) return DB_TABLE_NOT_EXIST;
  table_id_t tid = it->second;
  
  auto it2 = index_names_.find(table_name);
  for(auto it3 : it2->second){
    DropIndex(table_name, it3.first);
  }
  
  index_names_.erase(table_name);
  tables_.erase(tid);
  page_id_t page_id = catalog_meta_->table_meta_pages_[tid];
  catalog_meta_->table_meta_pages_.erase(tid);
  buffer_pool_manager_->DeletePage(page_id);
  table_names_.erase(table_name);
  // ASSERT(false, "Not Implemented yet");
  auto len = catalog_meta_->GetSerializedSize();
  char meta[len+1];
  catalog_meta_->SerializeTo(meta);
  char* p = buffer_pool_manager_->FetchPage(0)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p,meta,len);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto it = index_names_.find(table_name);
  if(it == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it2 = it->second.find(index_name);
  if(it2 == it->second.end()) return DB_INDEX_NOT_FOUND;
  
  index_id_t index_id = it2->second;
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  // IndexInfo* iinfo = indexes_[index_id];
  // delete iinfo;//因为iinfo中所有成员变量空间都是有heap_分配的，所以删除heap_即可
  it->second.erase(index_name);
  indexes_.erase(index_id);
  buffer_pool_manager_->DeletePage(page_id);

  auto len = catalog_meta_->GetSerializedSize();
  char meta[len+1];
  catalog_meta_->SerializeTo(meta);
  char* p = buffer_pool_manager_->FetchPage(0)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p,meta,len);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto it = tables_.find(table_id);
  if(it==tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it->second;
  return DB_SUCCESS;
}