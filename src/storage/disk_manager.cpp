#include <stdexcept>
#include <sys/stat.h>
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);//从page中读出来的内容存在page_data中
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  DiskFileMetaPage *MetaData=reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t extent_num = MetaData->num_extents_;
  //查看现在加到那个extent了
  uint32_t extent_id = 0;
  bool flag = false;
  for ( ; extent_id  < extent_num ; extent_id++ ){
    if (MetaData->extent_used_page_[extent_id]!=PageNum){
      flag = true;
      break;
    }
  }
  MetaData->num_allocated_pages_++;
  uint32_t page_offset ;
  if (flag){
    char NowBitMap[PAGE_SIZE];
    //从磁盘中读出该entent的bitmap
    ReadPhysicalPage((PageNum+1)*extent_id+1,NowBitMap);
    //分配地方并修改bitmap
    (reinterpret_cast<BitmapPage<PAGE_SIZE>*>(NowBitMap))->AllocatePage(page_offset);
    MetaData->extent_used_page_[extent_id]++;
    //将修改过的bitmap写回磁盘*/
    WritePhysicalPage((PageNum+1)*extent_id+1,NowBitMap);
  }
  else{
    MetaData->num_extents_++;
    MetaData->extent_used_page_[extent_id]=1;
    BitmapPage<PAGE_SIZE>*NewBitMap = new BitmapPage<PAGE_SIZE>;
    NewBitMap->AllocatePage(page_offset);
    WritePhysicalPage((PageNum+1)*extent_id+1,reinterpret_cast<char *>(NewBitMap));
  }
  return page_offset+PageNum*extent_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t extent_id = logical_page_id/PageNum;
  uint32_t page_offset = logical_page_id%PageNum;
  //从磁盘中读出该page所在的extent的bitmap
  char NowBitMap[PAGE_SIZE];
  ReadPhysicalPage((PageNum+1)*extent_id+1,NowBitMap);
  if ((reinterpret_cast<BitmapPage<PAGE_SIZE>*>(NowBitMap))->DeAllocatePage(page_offset)==false){
    return;
  }
  WritePhysicalPage((PageNum+1)*extent_id+1,NowBitMap);
  DiskFileMetaPage *MetaData=reinterpret_cast<DiskFileMetaPage *>(meta_data_) ;
  MetaData->num_allocated_pages_--;
  MetaData->extent_used_page_[extent_id]--;

}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t extent_id =logical_page_id/PageNum;//计算属于哪个entent
  char NotBitMap[PAGE_SIZE];
  ReadPhysicalPage((PageNum+1)*extent_id+1,NotBitMap);//从磁盘中读出该entent的bitmap
  //调用BitmapPage类的函数判断该page是否空闲
  return (reinterpret_cast<BitmapPage<PAGE_SIZE>*>(NotBitMap)->IsPageFree(logical_page_id%PageNum));
}

/*将逻辑页号转化成物理页号*/
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  //每一个extent中包含一个bitmap和PageNum个page
  return logical_page_id+logical_page_id/PageNum+2;
  /* (logical_page_id+1)/PageNum 计算了前面有多少个extent
  由于每个extent有一个bitmap，故物理页号要加上这些bitmap
  此外最开头还有个disk的meta，占一个map，故加1*/
  
}
/*得到文件的大小*/
int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}