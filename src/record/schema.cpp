#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  char* pos=buf;
  for(int i=0;i<int(columns_.size());i++){
    pos+=columns_[i]->SerializeTo(pos);
  }
  return pos-buf;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t size = 0;
  for(int i=0;i<int(columns_.size());i++){
    size+=columns_[i]->GetSerializedSize();
    //Sum of Each Column's Serialize Size
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  char* pos = buf;
  std::vector<Column*> columns;
  while(*pos!=0){
    Column* col;
    pos+=Column::DeserializeFrom(pos,col,heap);
    columns.push_back(col);
  }
  void *mem = heap->Allocate(sizeof(Schema));
  //Allocate a new memory for schema.
  schema = new(mem)Schema(columns);
  return pos-buf;
}