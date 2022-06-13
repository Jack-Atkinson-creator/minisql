#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  //table_id_(unit32);table_name_(uint32_t+string);root_page_id_(uint_32t);schema(Serialize it)

  char* pos=buf;
  memcpy(pos,&table_id_,sizeof(uint32_t));
  pos+=sizeof(uint32_t);
  uint32_t tablename_len=table_name_.size();//table_name size into pos
  memcpy(pos,&tablename_len,sizeof(uint32_t));
   pos+=sizeof(uint32_t);
   table_name_.copy(pos,tablename_len,0);//table_name into pos
   pos+=tablename_len;
   memcpy(pos,&root_page_id_,sizeof(uint32_t));
   pos+=sizeof(uint32_t);
   pos+=schema_->SerializeTo(pos);//Add Length of (serialized schema) into pos
   return pos-buf;
}

uint32_t TableMetadata::GetSerializedSize() const {

  return sizeof(uint32_t)*3+table_name_.size()+schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
     // use the heap in TableInfo.
    char*pos=buf;
    uint32_t TableID=MACH_READ_FROM(uint32_t,pos);pos+=sizeof(uint32_t);
    uint32_t stringlen=MACH_READ_FROM(uint32_t,pos);pos+=sizeof(uint32_t);
    std::string TableName;
     TableName.append(pos,stringlen);//Update string
     pos+=stringlen;
    uint32_t RootPageID=MACH_READ_FROM(uint32_t,pos);pos+=sizeof(uint32_t);
    Schema*schema=NULL;
    pos+=Schema::DeserializeFrom(pos,schema,heap);//new schema

    void *mem=heap->Allocate(sizeof(TableMetadata));//Allocate space for TableMetaData
   table_meta = new(mem)TableMetadata(TableID,TableName,RootPageID,schema);
   return pos-buf;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
