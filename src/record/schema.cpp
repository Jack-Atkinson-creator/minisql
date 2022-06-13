#include "record/schema.h"
uint32_t Schema::SerializeTo(char *buf) const {
  char*s = buf;
  MACH_WRITE_UINT32(buf,columns_.size());
  // //LOG(INFO) << columns_.size();
  buf+=sizeof(uint32_t);
  for(auto c:columns_)
    buf += c->SerializeTo(buf);
  return buf-s;
}

uint32_t Schema::GetSerializedSize() const {
  int offset = sizeof(int);
  for(auto c:columns_){
    // //LOG(INFO) << "c:" << c->GetName();
    offset += c->GetSerializedSize();
  }
  return offset; 
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  char*s = buf;
  int length = MACH_READ_INT32(buf);
  std::vector<Column*>columns;
  buf+=sizeof(int);
  // //LOG(INFO) << length;
  for(int i = 0;i<length;++i){
    Column*n = nullptr;
    buf+=Column::DeserializeFrom(buf,n,heap);
    columns.push_back(n);
    // //LOG(INFO) << n->GetName();
    // //LOG(INFO)<< n->GetTableInd();
    // //LOG(INFO) << n->GetType();
  }
  
  //  //LOG(INFO) << "GET";
  // //LOG(INFO) << sizeof(Schema);
  schema = ALLOC_P(heap,Schema)(columns);
  // //LOG(INFO) << sizeof(columns.size());
  // schema = new(heap->Allocate(columns.size()*sizeof(Column*)+sizeof(int)))Schema(columns);
  // //LOG(INFO) << "dddd";
  int offset = buf-s;
  return offset;
}
