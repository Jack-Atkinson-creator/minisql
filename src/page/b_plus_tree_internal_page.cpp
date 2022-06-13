#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
   SetPageId(page_id);
   SetParentPageId(parent_id);
   SetMaxSize(max_size);
   SetSize(0);
   SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // INDEX must be at [0,size);
  assert(index<GetSize()&&index>=0);
  return array_[index].first;//return key
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    // INDEX must be at (0,maxsize];
  assert(index > 0 && index <= GetMaxSize());
  array_[index].first=key;//Update the key.
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key,const KeyComparator &comparator)const
 {
        //not found :-1
        int left=1,right=GetSize()-1,mid=0;
        while(left<=right)
        {
            mid=(right+left)/2;
            int Comp=comparator(key,array_[mid].first);
         if(Comp<0)
             right=mid-1;
          else if(Comp>0)
             left=mid+1;
          else//Find exactly
            return mid;
  }
  return -1;
}
/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int i=0;
  while(i<GetSize())
  {
    //Compare the value
    if(array_[i].second==value)
       return i; //Found
    i++;
  }
   //Not found
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  
  // INDEX must be at [0,size);
  assert(index<GetSize()&&index>=0);
  return array_[index].second;//return value
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  //binary search
  int LeftBound=1,RightBound=GetSize()-1;
  while(LeftBound<=RightBound)
  {
      int mid=(LeftBound+RightBound)/2;
      int Comp=comparator(key,array_[mid].first);
      if(Comp<0)
        RightBound=mid-1;
      else if(Comp>0)
      {
          LeftBound=mid+1;
      }
      else//Find exactly
         return array_[mid].second;
  }
  return array_[RightBound].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) 
{
  //new root have 2 pointers
  this->SetSize(2);
   //array[0]'s pointer,array[1]'s val and pointer
    array_[0].second=old_value;
    array_[1].first = new_key;
    array_[1].second = new_value;
    
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) 
{
  int old_value_index = ValueIndex(old_value);
  assert(old_value_index!=-1);
  //Push back one unit.
  for (int i = GetSize(); i > old_value_index+1; i--) {
    array_[i].first = array_[i-1].first;
    array_[i].second = array_[i-1].second;
  }
  array_[old_value_index+1].first = new_key;
  array_[old_value_index+1].second = new_value;
  //add size by 1
  this->IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager)
{
   if(GetSize()>=GetMaxSize()+1)
   //size is too large
   {
            // Copy me into recipient 
    int ends = GetSize() - 1;
    int starts = ends / 2 + 1;
    recipient->CopyNFrom(&array_[starts],ends-starts+1,buffer_pool_manager);
    //Update size
    SetSize(starts);
   }

}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
    for(int i=0;i<size;i++){
      array_[i].first=items[i].first;
      array_[i].second=items[i].second;     
    }
    SetSize(size);
     //changing their parent page id to me.
    for (int i = 0; i < size; i++) {
        auto pageID = ValueAt(i);
        Page* page = buffer_pool_manager->FetchPage(pageID);
        BPlusTreePage *BPTP = reinterpret_cast<BPlusTreePage *>(page->GetData());
        BPTP->SetParentPageId(GetPageId());        //Let the parent become me.
        buffer_pool_manager->UnpinPage(pageID, true); //Unpin the current page.
    } 
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) 
{
    // INDEX must be at [0,size);
  assert(index<GetSize()&&index>=0);
  IncreaseSize(-1);
  for(int j=index;j<GetSize();j++)
  {
    array_[j]=array_[j+1];
    //Move forward for the data which >=index.
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    IncreaseSize(-1);
    return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) 
 //ASSUME the key in recipient is SMALLER than this.
 //Move the bigger node into the smaller one.
{
  //assure this and recipient are siblings
  assert(GetParentPageId()==recipient->GetParentPageId());
    //assure not overflow
     assert(recipient->GetSize()+GetSize()<=GetMaxSize());
    Page *pt = buffer_pool_manager->FetchPage(GetParentPageId()); //Get the parent page
    BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*ParentPage=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(pt->GetData());
  //We assure that the key in recipient is SMALLER than this.
    assert(ParentPage->ValueIndex(recipient->GetPageId()) < ParentPage->ValueIndex(this->GetPageId()));
   //this 40 ,recipient 36
    int indexParent=ParentPage->ValueIndex(GetPageId());//Find the index in parent node.
    array_[0].first = ParentPage->KeyAt(indexParent);
    buffer_pool_manager->UnpinPage(GetParentPageId(), false);
    //Copy the data in this page into recipient page.
    recipient->CopyAllFrom(array_,GetSize(),buffer_pool_manager);

    //Adjust the parent node->recipient
    for (int i = 0; i < GetSize(); i++) {
        page_id_t P1 = ValueAt(i);//Get the child page id.
        pt = buffer_pool_manager->FetchPage(P1);
         BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator> *ChildrenPage = reinterpret_cast< BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(pt->GetData());

        ChildrenPage->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(ChildrenPage->GetPageId(), true);//Unpin the child_page.
    }

    this->SetSize(0);//End this node.

    buffer_pool_manager->UnpinPage(GetPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    assert(GetSize() + size <= GetMaxSize());
    //Not exceed the max range!
    int begins = GetSize();
    for (int i = 0; i < size; ++i) {
        array_[begins + i] = *items++;
    }
    IncreaseSize(size);
}




/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, 
                                                      BufferPoolManager *buffer_pool_manager) 
{
  //this page and recipient page must be siblings.
    assert(GetParentPageId() == recipient->GetParentPageId());
    MappingType tempPair{KeyAt(1), ValueAt(0)}; 
    page_id_t TochangePageID = ValueAt(0); //the page whose parent should be changed.
    array_[0].second = ValueAt(1);//value[0]->value[1]  
    Remove(1);
    recipient->CopyLastFrom(tempPair, buffer_pool_manager);
  //Fetch page and change types
    Page*P0 = buffer_pool_manager->FetchPage(TochangePageID);
    auto TochangePage = reinterpret_cast<BPlusTreePage *>(P0->GetData());
  TochangePage->SetParentPageId(recipient->GetPageId());
//Unpin all pages
    buffer_pool_manager->UnpinPage(TochangePage->GetPageId(), true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
    buffer_pool_manager->UnpinPage(this->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) 
{
    Page*p=buffer_pool_manager->FetchPage(GetParentPageId());
    BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*parent_page=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p->GetData());
    int IndOfThis=parent_page->ValueIndex(GetPageId());
    IndOfThis++;
    array_[GetSize()].first=parent_page->KeyAt(IndOfThis);
    array_[GetSize()].second=pair.second;
   this-> IncreaseSize(1);
    parent_page->SetKeyAt(IndOfThis,pair.first);
    //Unpin the parent page.
    buffer_pool_manager->UnpinPage(parent_page->GetPageId(),true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, 
                                                       BufferPoolManager *buffer_pool_manager,int index)
{

      //this page and recipient page must be siblings.
    assert(GetParentPageId() == recipient->GetParentPageId());
    MappingType finally=array_[GetSize()-1];
    Remove(GetSize()-1);
    int32_t childID=finally.second;
    recipient->CopyFirstFrom(finally,buffer_pool_manager,index);
    //Change the parent.
    Page*p2=buffer_pool_manager->FetchPage(childID);
    BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*ToChangePage=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p2->GetData());
    ToChangePage->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(ToChangePage->GetPageId(),true);
    buffer_pool_manager->UnpinPage(this->GetPageId(),true);
    buffer_pool_manager->UnpinPage(recipient->GetPageId(),true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager,int index)
{
    //Fetch the parent
    Page*p1=buffer_pool_manager->FetchPage(GetParentPageId());
    BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*MyParent=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p1->GetData());
    auto par_key=MyParent->KeyAt(index);//par_key
    //Update the parent node.
   MyParent->SetKeyAt(index,pair.first); 
   //Copy to the first val.
    InsertNodeAfter(array_[0].second, par_key, array_[0].second);
    array_[0].second=pair.second;
    buffer_pool_manager->UnpinPage(GetParentPageId(),true);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;