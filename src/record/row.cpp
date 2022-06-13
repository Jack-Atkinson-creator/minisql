#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  //
  //printf("Row serialize To calls\n");

  char *temp = buf;
  // size
  MACH_WRITE_UINT32(temp, fields_.size());
  temp += sizeof(uint32_t);
  // judge ifn exist
  for (size_t i = 0; i < fields_.size(); i++) {
    MACH_WRITE_TO(bool, temp, fields_[i]->IsNull());
    temp += sizeof(bool);
  }
  // data
  for (size_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull() == false) {
      fields_[i]->SerializeTo(temp);
      temp += fields_[i]->GetSerializedSize();
    }
  }
  // how many bytes forward
  return temp - buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {

  //
  // printf("Row::DeserializeFrom Calles\n");
   ///
  char *temp = buf;
  // size
  uint32_t size = MACH_READ_UINT32(temp);
  temp += sizeof(uint32_t);
  // ifn exist
  bool *null_map = new bool[size];
  for (size_t i = 0; i < size; i++) {
    null_map[i] = MACH_READ_FROM(bool, temp);
    temp += sizeof(bool);
  }
  // read data
  for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
    if (null_map[i] == false) {
      fields_.push_back(nullptr);
      fields_[i]->DeserializeFrom(temp, schema->GetColumn(i)->GetType(), &(fields_[i]), null_map[i], heap_);
      temp += fields_[i]->GetSerializedSize();
    } 
    else {
      void *mem_alloc = heap_->Allocate(sizeof(Field));
      new (mem_alloc) Field(schema->GetColumn(i)->GetType());
      fields_.push_back((Field *)mem_alloc);
    }
  }
  return temp - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t Size = sizeof(uint32_t) + sizeof(bool) * fields_.size();
  for (size_t i = 0; i < fields_.size(); ++i) {
    if (fields_[i]->IsNull() == false) Size += fields_[i]->GetSerializedSize();
  }
  return Size;
}
