#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const
{
  //SizeOf table_meta_pages_(uint32_t)|n <table_id_t, page_id_t>|SizeOf index_meta_pages_(uint32_t)|m <index_id_t, page_id_t>
  char*pos=buf;
  uint32_t TableMetaPageSize=table_meta_pages_.size();
   memcpy(pos,&TableMetaPageSize,sizeof(uint32_t));
   pos+=sizeof(uint32_t);
  std::map<table_id_t, page_id_t>::const_iterator it;
   for( it= table_meta_pages_.cbegin();it!=table_meta_pages_.cend();it++)
   {
           table_id_t tid=(*it).first;
           page_id_t pid=(*it).second;
            memcpy(pos,&tid,sizeof(table_id_t));
            pos+=sizeof(table_id_t);
            memcpy(pos,&pid,sizeof(page_id_t));
            pos+=sizeof(page_id_t);
   }
   uint32_t IndexMetaSize=index_meta_pages_.size();
   memcpy(pos,&IndexMetaSize,sizeof(uint32_t));
   pos+=sizeof(uint32_t);
    std::map<index_id_t, page_id_t> ::const_iterator it2;
    for(it2=index_meta_pages_.cbegin();it2!=index_meta_pages_.cend();it2++)
    {      
          index_id_t ind=(*it2).first;page_id_t pid=(*it2).second;
           memcpy(pos,&ind,sizeof(index_id_t));
            pos+=sizeof(index_id_t);
            memcpy(pos,&pid,sizeof(page_id_t));
            pos+=sizeof(page_id_t);
    }
    return;
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
    void *mem=heap->Allocate(sizeof(CatalogMeta));
    CatalogMeta*NewCatalogMeta=new(mem)CatalogMeta();//Create a new catalog meta.
   char*pos=buf;
  uint32_t TableMetaPageSize=MACH_READ_FROM(uint32_t,pos);
   pos+=sizeof(uint32_t);//
  
   for(int i=0;i<int(TableMetaPageSize);i++)
   {
      table_id_t tid=MACH_READ_FROM(table_id_t,pos);
      pos+=sizeof(table_id_t);
      page_id_t pid=MACH_READ_FROM(page_id_t,pos);
      pos+=sizeof(page_id_t);
      //insert <tid,pid> into map;
      NewCatalogMeta->GetTableMetaPages()->emplace(tid,pid);
    }
   uint32_t IndexMetaPageSize=MACH_READ_FROM(uint32_t,pos);
   pos+=sizeof(uint32_t);
   for(int i=0;i<int(IndexMetaPageSize);i++)
   {
      index_id_t iid=MACH_READ_FROM(index_id_t,pos);
      pos+=sizeof(index_id_t);
      page_id_t ppid=MACH_READ_FROM(page_id_t,pos);
      pos+=sizeof(page_id_t);
      //insert <iid,ppid> into map
      NewCatalogMeta->GetIndexMetaPages()->emplace(iid,ppid);
   }
  return NewCatalogMeta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t TableMetaSize=table_meta_pages_.size();
  uint32_t IndexMetaSize=index_meta_pages_.size();
  return 2*sizeof(uint32_t)+TableMetaSize*(sizeof(page_id_t)+sizeof(table_id_t))+IndexMetaSize*(sizeof(page_id_t)+sizeof(index_id_t));
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  // ASSERT(false, "Not Implemented yet");

  //printf("CatalogManager Start\n");
   if(init){
    catalog_meta_ = new(heap_->Allocate(sizeof(CatalogMeta)))CatalogMeta;
  }
  else 
  {   
    //Assure that New page cannot encounter 0 or 1.
    
    Page* p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char* t = p->GetData();
    catalog_meta_ = CatalogMeta::DeserializeFrom(t,heap_);
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    for(auto it:catalog_meta_->table_meta_pages_){
     dberr_t error= LoadTable(it.first,it.second);
     assert(error==DB_SUCCESS);
    }
    for(auto it:catalog_meta_->index_meta_pages_){
       dberr_t error=LoadIndex(it.first,it.second);
      assert(error==DB_SUCCESS);
    }
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,false);
  }
  
  //printf ("CatalogManager End\n");
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  
  // ASSERT(false, "Not Implemented yet");
  //judge if the table has already existed.
  if(table_names_.find(table_name)!=table_names_.end())
  //Exist
  {
      return DB_TABLE_ALREADY_EXIST;
  } 
   //Create a table named table_name
  /* table_info is created by CatalogManager using CatalogManager's heap_ */
     
    table_id_t table_id = next_table_id_++;
    page_id_t page_id;//Table MetaData的数据页
    Page* new_table_page = buffer_pool_manager_->NewPage(page_id);
    catalog_meta_->table_meta_pages_[table_id] = page_id;
   // catalog_meta_->table_meta_pages_[next_table_id_] = -1;
    
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,schema, txn,log_manager_, lock_manager_, heap_);
    
     TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema, heap_);
    //TableMetadata::root_page_id:: Record's first page id.
    
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
    memcpy(p,meta,len);//Copy table_meta into newpage's data.
    buffer_pool_manager_->UnpinPage(new_table_page->GetPageId(),true);
    //Update catalog_meta_
    len = catalog_meta_->GetSerializedSize();
    char cmeta[len+1];
    catalog_meta_->SerializeTo(cmeta);
    p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
    memset(p, 0, PAGE_SIZE);
    memcpy(p,cmeta,len);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
    if(table_meta!=nullptr && table_heap!=nullptr)
      return DB_SUCCESS;
    return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(table_names_.find(table_name)==table_names_.end())
 //Cannot find the table 
  {
     return DB_TABLE_NOT_EXIST;
  } 
  //Find the table
  table_id_t tid=table_names_[table_name];
  table_info=tables_[tid];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
     std::unordered_map<table_id_t, TableInfo *> ::const_iterator iter;
    for(iter=tables_.cbegin();iter!=tables_.cend();iter++)
    {
        tables.push_back(iter->second);
    }
    return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {

   if(index_names_.count(table_name)<=0) 
       return DB_TABLE_NOT_EXIST;
    auto it = index_names_[table_name];
    if(it.count(index_name)>0) 
       return DB_INDEX_ALREADY_EXIST;

    index_id_t index_id = next_index_id_++;
    std::vector<uint32_t> key_map;
    TableInfo* tinfo;
    dberr_t error = GetTable(table_name,tinfo);
    if(error)
      return error;
    for(int j=0;j<int(index_keys.size());j++){
      auto i = index_keys[j];
      uint32_t tkey;
      dberr_t err = tinfo->GetSchema()->GetColumnIndex(i, tkey);
      if(err) 
         return err;
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
    catalog_meta_->index_meta_pages_[index_id] = page_id;//Add one record.
    //catalog_meta_->index_meta_pages_[next_index_id_] = -1;
    
    auto len = index_meta_data_ptr->GetSerializedSize();
    char meta[len+1];
    index_meta_data_ptr->SerializeTo(meta);
    char* p = new_index_page->GetData();
    memcpy(p,meta,len);
     buffer_pool_manager_->UnpinPage(new_index_page->GetPageId(),true);

    len = catalog_meta_->GetSerializedSize();
    char cmeta[len+1];
    catalog_meta_->SerializeTo(cmeta);
    p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
    memset(p, 0, PAGE_SIZE);
    memcpy(p,cmeta,len);   
    //Unpin the page
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
   return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");  
  if (index_names_.find(table_name)== index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto index_table = index_names_.find(table_name)->second;
  auto search_index_id = index_table.find(index_name);
  if (search_index_id == index_table.end()){
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = search_index_id->second;
  auto it = indexes_.find(index_id);
  if(it == indexes_.end())
     return DB_INDEX_NOT_FOUND;
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
    index_id_t Index_ID=it.second;
    indexes.push_back(indexes_.find(Index_ID)->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it = table_names_.find(table_name);
  if(it == table_names_.end()) 
      return DB_TABLE_NOT_EXIST;
  table_id_t tid = it->second;
  
  auto it2 = index_names_.find(table_name);
  for(auto it3 : it2->second){
    //Drop all the indexes in the table.
    DropIndex(table_name, it3.first);
  }
  
  index_names_.erase(table_name);
  tables_.erase(tid);
  page_id_t page_id = catalog_meta_->table_meta_pages_[tid];
  catalog_meta_->table_meta_pages_.erase(tid);
  buffer_pool_manager_->DeletePage(page_id);//Delete table_meta_pages_ID
  table_names_.erase(table_name);
  // ASSERT(false, "Not Implemented yet");
  auto len = catalog_meta_->GetSerializedSize();
  char meta[len+1];
  catalog_meta_->SerializeTo(meta);
  char* p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p,meta,len);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
 // ASSERT(false, "Not Implemented yet");
  auto it = index_names_.find(table_name);
  if(it == index_names_.end()) 
     return DB_TABLE_NOT_EXIST;
  auto it2 = it->second.find(index_name);
  if(it2 == it->second.end()) 
     return DB_INDEX_NOT_FOUND;
  
  index_id_t index_id = it2->second;
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  catalog_meta_->index_meta_pages_.erase(index_id);
  // IndexInfo* iinfo = indexes_[index_id];
  // delete iinfo;
  it->second.erase(index_name);
  indexes_.erase(index_id);
  buffer_pool_manager_->DeletePage(page_id);//Delete index_meta_page_id.

  auto len = catalog_meta_->GetSerializedSize();
  char meta[len+1];
  catalog_meta_->SerializeTo(meta);
  char* p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData();
  memset(p, 0, PAGE_SIZE);
  memcpy(p,meta,len);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // ASSERT(false, "Not Implemented yet");
  
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)==true)
  //Write the information in catalog meta page into the disk.
  {
    return DB_SUCCESS;
  }
    return DB_FAILED;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
     if(catalog_meta_->table_meta_pages_.find(table_id)==catalog_meta_->table_meta_pages_.end())
     //Table Not exist.
     {
         return DB_TABLE_NOT_EXIST;
     }
     
      Page*p = buffer_pool_manager_->FetchPage(page_id);
      char*t = p->GetData();
      TableMetadata* meta;    
      meta->DeserializeFrom(t,meta, heap_); 
      TableInfo* tinfo;
      tinfo = TableInfo::Create(heap_);

      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,meta->GetFirstPageId() ,meta->GetSchema(), log_manager_, lock_manager_, heap_);
      tinfo->Init(meta, table_heap);
      table_names_[meta->GetTableName()] = meta->GetTableId();
      tables_[meta->GetTableId()] = tinfo;
      index_names_.insert({meta->GetTableName(), std::unordered_map<std::string, index_id_t>()});
       buffer_pool_manager_->UnpinPage(page_id,false);
    return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
   if(catalog_meta_->index_meta_pages_.find(index_id)==catalog_meta_->index_meta_pages_.end())
   {
     return DB_INDEX_NOT_FOUND;
   }
      Page*p = buffer_pool_manager_->FetchPage(page_id);
      char*t = p->GetData();
      IndexMetadata* meta;
      IndexMetadata::DeserializeFrom(t, meta, heap_);
      TableInfo* tinfo = tables_[meta->GetTableId()];
      IndexInfo* index_info = IndexInfo::Create(heap_);
      index_info->Init(meta,tinfo,buffer_pool_manager_);//segmentation
      index_names_[tinfo->GetTableName()][meta->GetIndexName()] = meta->GetIndexId();
      indexes_[meta->GetIndexId()] = index_info;
      buffer_pool_manager_->UnpinPage(page_id,false);
       return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  if(tables_.find(table_id)==tables_.end())
  {
    return DB_TABLE_NOT_EXIST;
  }
  table_info=tables_[table_id];
  return DB_SUCCESS;
}