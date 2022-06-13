#include "page/bitmap_page.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (next_free_page_ < MAX_CHARS * 8) {  // not full yet, can allocate another page
    page_allocated_++;
    page_offset = next_free_page_;

    // update bytes
    uint8_t bit_index = next_free_page_ % 8;
    uint32_t byte_index = next_free_page_ / 8;
    uint8_t change = 1 << bit_index;
    bytes[byte_index] |= change;

    // update next_page_free
    uint32_t i = next_free_page_;
    for (; i < MAX_CHARS * 8; i++) {
      if (IsPageFree(i) == true) {
        break;
      }
    }
    next_free_page_ = i;

    return true;
  } else {  // already full
    return false;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint8_t bit_index = page_offset % 8;
  uint32_t byte_index = page_offset / 8;
  uint8_t change = 1 << bit_index;
  if ((bytes[byte_index] & change) == 0) {  // the bit is already 0
    return false;
  } else {
    page_allocated_--;
    bytes[byte_index] ^= change;

    if (page_offset < next_free_page_) {
      next_free_page_ = page_offset;
    }
    return true;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint8_t bit_index = page_offset % 8;
  uint32_t byte_index = page_offset / 8;
  uint8_t change = 1 << bit_index;
  if ((bytes[byte_index] & change) == 0) {  // the bit is already 0
    return true;
  } else {
    return false;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
