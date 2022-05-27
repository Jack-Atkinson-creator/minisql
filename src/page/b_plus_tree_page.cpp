#include "page/b_plus_tree_page.h"

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const {
  return IndexPageType::LEAF_PAGE==page_type_;   
}

bool BPlusTreePage::IsRootPage() const {
  //no parent->root
  return INVALID_PAGE_ID==parent_page_id_;
}

void BPlusTreePage::SetPageType(IndexPageType page_type) {
   this->page_type_=page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const {
  return size_;
}

void BPlusTreePage::SetSize(int size) {
   this->size_=size;
}

void BPlusTreePage::IncreaseSize(int amount) {
   this->size_+=amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const {
  return max_size_;
}

void BPlusTreePage::SetMaxSize(int size) {
   this->max_size_=size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const {
  if(IsRootPage()==true && IsLeafPage()==true)
  //root and leaf
  {
    return 1;
  }
  if(IsRootPage()) //root-> 2 ptrs
       return 2;
      //MinPageSize=UpperBound(MaxPageSize/2)
   else if(GetMaxSize()%2==0)//max_pointer number is even.
   {
       return  GetMaxSize()/2;
   }
   else //max_pointer number is odd.
    return GetMaxSize()/2 +1;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const {

  return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
   this->parent_page_id_=parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const {

  return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id) {
   this->page_id_=page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) {
  this->lsn_ = lsn;
}