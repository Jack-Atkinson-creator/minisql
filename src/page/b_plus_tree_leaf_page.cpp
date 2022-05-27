#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) 
{
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetSize(0);
    SetPageType(IndexPageType::LEAF_PAGE);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {

  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
    this->next_page_id_=next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
     int low=0,high=GetSize()-1;
     //binary search
     while(low<=high)
     {
       int middle=((high-low)/2)+low; 
       int comp=comparator(array_[middle].first,key);
       if(comp>0)
       {
         high=middle-1;
       }  
        else if(comp<0)
        {
          low=middle+1;
        }
        else   //[middle]==key
        {
          return middle;
        }
     }
     return low;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  //index must be at [0,size)
  assert(index<GetSize()&&index>=0);
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
   //index must be at [0,size)
  assert(index<GetSize()&&index>=0);
   return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  //the first index >=key
    int FirstIndex = KeyIndex(key, comparator);

    for (int i = GetSize() - 1; i >= FirstIndex; i--) {
        array_[i + 1].first = array_[i].first;
        array_[i + 1].second = array_[i].second;
    }
    array_[FirstIndex].second = value;
    array_[FirstIndex].first = key;
    IncreaseSize(1);
    //return page size.
    int CurrSize=GetSize();
    return CurrSize;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient)
{
  //NEED TO SPLIT
    assert(GetMaxSize()+1==GetSize());
    //Insert recipient after this
    recipient->SetNextPageId(this->GetNextPageId());
    SetNextPageId(recipient->GetPageId());

    int End=GetSize()-1;
    int Begin=End/2+1;
    //array_[Begin]~[End]----->recipient
    recipient->CopyNFrom(&array_[Begin],End-Begin+1);
    SetSize(Begin);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
   
   for(int i=0;i<size;i++)
   {
     array_[i].first=items[i].first;
     array_[i].second=items[i].second;
   }
   SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,const KeyComparator &comparator)const
{
  //Empty Node
  if(GetSize()==0)return false;
  
  int i=KeyIndex(key,comparator);
   if(i<GetSize() && comparator(key, array_[i].first)==0)//array_[i].first------KeyType 
   {   
      //the key exactly exists in the leaf page.     
        value=array_[i].second;
        return true;
   }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator)
{
   int i=KeyIndex(key,comparator);
   if(i<GetSize())
   {
      if(comparator(GetItem(i).first,key)==0) //the key exactly exists in the leaf page.
      {
        //remove the key.
        //The elements behind will move forward one unit.
        for(int j=i;j<GetSize()-1;j++)
        {
          array_[j].first=array_[j+1].first;
          array_[j].second=array_[j+1].second;
        }
        IncreaseSize(-1);
      }

   }
  //Return page size.
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,const KeyType &middle_key,BufferPoolManager *bufferpoolmanager)
{ 
  //We need to assure that the size cannot exceed the MAXSIZE.
    assert(recipient->GetSize() + this->GetSize() <= GetMaxSize()); 
    //Assume "recipient" page is prior of this page,and they are siblings.
    assert( GetPageId()==recipient->GetNextPageId()&&GetParentPageId() == recipient->GetParentPageId());
    
    int begin=recipient->GetSize();

   for(int i=0;i<GetSize();i++)
   {
      recipient->array_[begin+i].first=array_[i].first;
      recipient->array_[begin+i].second=array_[i].second;
   }
   //Adjust the size.
   recipient->IncreaseSize(this->GetSize());
    SetSize(0);
    recipient->SetNextPageId(this->GetNextPageId());
    SetNextPageId(INVALID_PAGE_ID);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient,BufferPoolManager *bufferpoolmanager)
{
 //Assume "recipient" page is prior of this page,and they are siblings.
    assert(GetParentPageId() == recipient->GetParentPageId()&&recipient->GetNextPageId() == GetPageId());
    MappingType firstElement=GetItem(0);
    recipient->CopyLastFrom(firstElement);
     
    //move forward a unit
    for(int i=0;i<GetSize()-1;i++){
      array_[i].first=array_[i+1].first;
      array_[i].second=array_[i+1].second;
    }
    IncreaseSize(-1);
    //Update parent's key
    Page*p=bufferpoolmanager->FetchPage(GetParentPageId());
    BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*ParPage=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p->GetData());
    int uin=ParPage->ValueIndex(GetPageId());
    ParPage->SetKeyAt(uin, GetItem(0).first);//the first element = parent's corresponding key.
   //Unpin the page we use.
    bufferpoolmanager->UnpinPage(GetParentPageId(),true);
    bufferpoolmanager->UnpinPage(recipient->GetPageId(), true);    
    bufferpoolmanager->UnpinPage(this->GetPageId(), true);
}  

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
    array_[GetSize()].first=item.first;
    array_[GetSize()].second=item.second;
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient,BufferPoolManager *bufferpoolmanager)
{
     //Assume "recipient" page is behind this page,and they are siblings.
     assert(GetParentPageId()==recipient->GetParentPageId()&& this->GetNextPageId()==recipient->GetPageId());
     MappingType finalElement=GetItem(GetSize()-1);
     IncreaseSize(-1);//Size--
     recipient->CopyFirstFrom(finalElement,bufferpoolmanager);
     
   //Unpin the page.
      bufferpoolmanager->UnpinPage(GetPageId(),true);
      bufferpoolmanager->UnpinPage(recipient->GetPageId(),true);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item,BufferPoolManager *bufferpoolmanager) 
{
   //move backward 1 unit
   for(int i=GetSize()-1;i>=0;i--)
   {
     array_[i+1]=array_[i];
   }
   array_[0]=item;
   IncreaseSize(1);
   Page*p=bufferpoolmanager->FetchPage(GetParentPageId());
   BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*ParPage=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p->GetData());
   //Get the index of this in parent.
   int pindex=ParPage->ValueIndex(GetPageId());
   ParPage->SetKeyAt(pindex, array_[0].first);
   bufferpoolmanager->UnpinPage(GetParentPageId(),true);
   
  }

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;