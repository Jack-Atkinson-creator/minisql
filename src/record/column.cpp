#include "record/column.h"
#include "common/macros.h"
#include "catalog/catalog.h"
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
   char*s = buf;
   MACH_WRITE_UINT32(buf,this->name_.length());
   buf+=sizeof(uint32_t);
   MACH_WRITE_STRING(buf,this->name_);
  //  std::cout << "35:" << this->name_ << std::endl;
   buf+=this->name_.length();
  // buf+=this->name_.length();
   MACH_WRITE_TO(TypeId,buf,this->type_);
   buf+=sizeof(TypeId);
   MACH_WRITE_UINT32(buf,this->len_);
   buf+=sizeof(uint32_t);
   MACH_WRITE_UINT32(buf,this->table_ind_);
   buf+=sizeof(uint32_t);
   MACH_WRITE_INT32(buf,this->nullable_);
   buf+=sizeof(int);
   MACH_WRITE_INT32(buf,this->unique_);
   buf+=sizeof(int);
  //  Column*c;
  //  std::cout << this->DeserializeFrom(s,c,nullptr) << std::endl;
   return buf-s;
}

uint32_t Column::GetSerializedSize() const {
  return 5*sizeof(int)+this->name_.length()+sizeof(TypeId);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  char*s = buf;
  int l = MACH_READ_UINT32(buf);
  // std::cout << l << std::endl;
  buf+=sizeof(int);
  char*tmp = new char[l+10];
  memcpy(tmp,buf,l);
  tmp[l] = 0;
  std::string name(tmp);
  delete[]tmp;
  // std::cout<< name<<std::endl;
  //delete tmp;
  buf+=l;
  TypeId type = MACH_READ_FROM(TypeId,buf);
  buf+=sizeof(TypeId);
  uint32_t len = MACH_READ_FROM(TypeId,buf);
  buf+=sizeof(uint32_t);
  uint32_t table_index = MACH_READ_FROM(TypeId,buf);
  buf+=sizeof(uint32_t);
  bool nullable = MACH_READ_INT32(buf);
  buf+=sizeof(int32_t);
  bool unique = MACH_READ_INT32(buf);
  buf+=sizeof(int32_t);
  // std::cout<<"fds"<<buf-s<<std::endl;
  if(type == kTypeChar)column = ALLOC_P(heap,Column)(name,type,len,table_index,nullable,unique);
  else column = ALLOC_P(heap,Column)(name,type,table_index,nullable,unique);
  return buf-s;
}


// #include "record/column.h"

// Column::Column(std::string col_name, TypeId type, uint32_t index, bool nullable, bool unique)
//         : name_(std::move(col_name)), type_(type), table_ind_(index),
//           nullable_(nullable), unique_(unique) {
//   ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
//   switch (type) {
//     case TypeId::kTypeInt :
//       len_ = sizeof(int32_t);
//       break;
//     case TypeId::kTypeFloat :
//       len_ = sizeof(float_t);
//       break;
//     default:
//       ASSERT(false, "Unsupported column type.");
//   }
// }

// Column::Column(std::string col_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
//         : name_(std::move(col_name)), type_(type), len_(length),
//           table_ind_(index), nullable_(nullable), unique_(unique) {
//   ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
// }

// Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
//                                       table_ind_(other->table_ind_), nullable_(other->nullable_),
//                                       unique_(other->unique_) {}

// uint32_t Column::SerializeTo(char *buf) const {
//   char *temp = buf;

//   MACH_WRITE_TO(uint32_t, temp, name_.size());
//   temp += sizeof(uint32_t);
//   MACH_WRITE_STRING(temp, name_);
//   temp += sizeof(char) * name_.length();
//   MACH_WRITE_TO(uint32_t, temp, type_);
//   temp += sizeof(uint32_t);
//   MACH_WRITE_TO(uint32_t, temp, len_);
//   temp += sizeof(uint32_t);
//   MACH_WRITE_TO(uint32_t, temp, table_ind_);
//   temp += sizeof(uint32_t);
//   MACH_WRITE_TO(uint32_t, temp, nullable_);
//   temp += sizeof(uint32_t);
//   MACH_WRITE_TO(uint32_t, temp, unique_);
//   temp += sizeof(uint32_t);

//   return GetSerializedSize();
// }

// uint32_t Column::GetSerializedSize() const {
//   uint32_t Size = 0;
//   Size += sizeof(uint32_t) * 6 + name_.length() * sizeof(char);
//   return Size;
// }

// uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
//   char *temp = buf;

//   void *mem_alloc = heap->Allocate(sizeof(Column));

//   uint32_t name_len = MACH_READ_FROM(uint32_t, temp);
//   temp += sizeof(uint32_t);

//   std::string col_name;
//   for (uint32_t i = 0; i < name_len; i++) {
//     char c = MACH_READ_FROM(char, temp);
//     temp += sizeof(char);
//     col_name += c;
//   }

//   TypeId type = (TypeId)MACH_READ_FROM(uint32_t, temp);
//   temp += sizeof(uint32_t);
//   uint32_t len = MACH_READ_FROM(uint32_t, temp);
//   temp += sizeof(uint32_t);
//   uint32_t col_ind = MACH_READ_FROM(uint32_t, temp);
//   temp += sizeof(uint32_t);
//   bool nullable = (bool)MACH_READ_FROM(uint32_t, temp);
//   temp += sizeof(uint32_t);
//   bool unique = (bool)MACH_READ_FROM(uint32_t, temp);
//   temp += sizeof(uint32_t);

//   // Called constructors vary by datatype
//   if (type == kTypeChar) 
//     column = new (mem_alloc) Column(col_name, type, len, col_ind, nullable, unique);
//   else if (type == kTypeInt || type == kTypeFloat)
//     column = new (mem_alloc) Column(col_name, type, col_ind, nullable, unique);
//   else column = new (mem_alloc) Column(nullptr);

//   return temp - buf;
// }

