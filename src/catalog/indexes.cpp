#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  //  index_id_(uint32_t)| (indexnamelen)(uint32_t)|index_name(string)|table_id(uint32_t)|key_map(uint32_t) number| n integer in the key_map
  char*pos=buf;
  memcpy(pos,&index_id_,sizeof(index_id_t));pos+=sizeof(index_id_t);
  uint32_t IndexNameLen=index_name_.size();
  memcpy(pos,&IndexNameLen,sizeof(uint32_t));pos+=sizeof(uint32_t);
  index_name_.copy(pos,IndexNameLen,0);
  pos+=IndexNameLen;
  memcpy(pos,&table_id_,sizeof(table_id_t));pos+=sizeof(table_id_t);
  uint32_t NumKeyMap=key_map_.size();
  memcpy(pos,&NumKeyMap,sizeof(uint32_t));pos+=sizeof(uint32_t);
  for(uint32_t i=0;i<NumKeyMap;i++)
  {
     MACH_WRITE_TO(uint32_t, pos,key_map_[i]);
     pos+=sizeof(uint32_t);
  }
  return pos-buf;
}

uint32_t IndexMetadata::GetSerializedSize() const 
{
   uint32_t NumKeyMap=key_map_.size();
   return sizeof(uint32_t)*4+index_name_.size()+NumKeyMap*sizeof(uint32_t);
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap)
{
     char*pos=buf;
     uint32_t indexid,indexNameLen,tableid,KeyMapSize;
     indexid=MACH_READ_FROM(uint32_t,pos);pos+=sizeof(uint32_t);
     indexNameLen=MACH_READ_FROM(uint32_t,pos);pos+=sizeof(uint32_t);
    //Update index_name  strings
    std::string IndexName;
    IndexName.append(pos,indexNameLen);
    pos+=indexNameLen;
     tableid=MACH_READ_FROM(uint32_t,pos);pos+=sizeof(uint32_t);
     KeyMapSize=MACH_READ_FROM(uint32_t,pos);pos+=sizeof(uint32_t);
     std::vector<uint32_t> keymap;
     for(uint32_t i=0;i<KeyMapSize;i++)
     {
        uint32_t tmp=MACH_READ_FROM(uint32_t,pos);
        pos+=sizeof(uint32_t);
        //push into vector
        keymap.push_back(tmp);
     }
    index_meta=IndexMetadata::Create(indexid,IndexName,tableid,keymap,heap);
    return pos-buf;
}