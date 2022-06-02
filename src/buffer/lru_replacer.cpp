#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { capacity = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.size() == 0) {  // empty
    return false;
  } else {
    *frame_id = lru_list_.back();
    lru_hash.erase(*frame_id);
    lru_list_.pop_back();
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (lru_hash.count(frame_id)) {
    auto p = lru_hash[frame_id];
    lru_hash.erase(frame_id);
    lru_list_.erase(p);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_hash.count(frame_id)) {             // has pinned
  } else if (lru_list_.size() == capacity) {  // full
    return;
  } else {
    lru_list_.push_front(frame_id);
    auto p = lru_list_.begin();
    lru_hash.emplace(frame_id, p);
  }
}

size_t LRUReplacer::Size() { return lru_list_.size(); }