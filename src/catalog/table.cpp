#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  char* buffer = buf;
  MACH_WRITE_TO(uint32_t, buffer, TABLE_METADATA_MAGIC_NUM);
  buffer+=sizeof(uint32_t);

  MACH_WRITE_TO(table_id_t, buffer,table_id_);
  buffer+=sizeof(uint32_t);

  uint32_t len = table_name_.size(); 
  memcpy(buffer, &len, sizeof(uint32_t));
  memcpy(buffer + sizeof(uint32_t), table_name_.c_str(), len);
  buffer = buffer + len + sizeof(uint32_t);
  
  MACH_WRITE_TO(int32_t, buffer,root_page_id_);
  buffer+=sizeof(int32_t);
  buffer+=schema_->SerializeTo(buffer);

  return buffer-buf;
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t ans=0;
  uint32_t len = table_name_.size();
  ans = 4*4+len+schema_->GetSerializedSize();
  return ans;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  table_id_t table_id;
  std::string table_name;
  page_id_t root_page_id;
  TableSchema *schema;
  char* buffer = buf;
  [[maybe_unused]]uint32_t val = MACH_READ_FROM(uint32_t, buffer); buffer+=4;//magic number
  table_id = MACH_READ_FROM(uint32_t, buffer); buffer+=4;

  uint32_t name_len = MACH_READ_FROM(uint32_t, buffer); buffer+=4;
  table_name.append(buffer,name_len);
  buffer+=name_len;

  root_page_id = MACH_READ_FROM(int32_t, buffer); buffer+=4;
  buffer+=schema->DeserializeFrom(buffer,schema,heap);

  void *mem = heap->Allocate(sizeof(TableMetadata));
  table_meta = new(mem)TableMetadata(table_id, table_name, root_page_id, schema);
  return buffer-buf;
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
