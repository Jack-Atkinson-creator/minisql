#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  frame_id_t P, R;
  
  if (page_table_.count(page_id) > 0) {  // If P exists, pin it and return it immediately.
    P = page_table_[page_id];
    replacer_->Pin(P);
    pages_[P].pin_count_++;
    return &pages_[P];
  }
  if (free_list_.size() > 0) {  // If P does not exist, find a replacement page (R) from free list.
    R = free_list_.front();
    page_table_[page_id] = R;
    free_list_.pop_front();
    disk_manager_->ReadPage(page_id, pages_[R].data_);
    pages_[R].pin_count_ = 1;
    pages_[R].page_id_ = page_id;
    return &pages_[R];
  } else {  // find a replacement page (R) from the replacer.
    if ((replacer_->Victim(&R)) == false) return nullptr;
    if (pages_[R].IsDirty()) {  // If R is dirty, write it back to the disk.
      disk_manager_->WritePage(pages_[R].GetPageId(), pages_[R].GetData());
    }
    // Delete R from the page table and insert P.
    pages_[R].page_id_ = page_id;
    pages_[R].pin_count_ = 1;
    page_table_[page_id] = R;
    // Update P's metadata, read in the page content from disk, and then return a pointer to P.
    disk_manager_->ReadPage(page_id, pages_[R].data_);
    return &pages_[R];
  }
  return nullptr;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id;
  if (free_list_.size() > 0) {  // Pick a victim page P from the free list
    page_id = AllocatePage();
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {  // free_list_ is empty. Pick a victim page P from the replacer

    if ((replacer_->Victim(&frame_id)) == false) return nullptr;
    page_id = AllocatePage();
    if (pages_[frame_id].IsDirty()) {  // If it is dirty, write it back to the disk.
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    page_table_.erase(pages_[frame_id].page_id_);
  }
  // Update P's metadata, zero out memory and add P to the page table.
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  page_table_[page_id] = frame_id;
  // Set the page ID output parameter. Return a pointer to P.
  return &pages_[frame_id];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
  // list.
  frame_id_t frame_id = page_table_[page_id];
  if (page_table_.count(page_id) == 0)
    return true;  // If P does not exist, return true.
  else if (pages_[frame_id].pin_count_ > 0)
    return false;  // If P exists, but has a non-zero pin-count, return false.
  // Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  free_list_.push_back(frame_id);
  disk_manager_->DeAllocatePage(page_id);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_table_.count(page_id) == 0) return false;  // does not exist
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) return true;  // has no pin, do not need any operation
  if (is_dirty) pages_[frame_id].is_dirty_ = true;
  replacer_->Unpin(frame_id);
  pages_[frame_id].pin_count_--;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();  // to protect shared data structure
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);  // writh the page into disk
  latch_.unlock();
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}