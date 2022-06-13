#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager*b,page_id_t pid,BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*lp,int index) 
:buff_pool_manager(b),CurrPageID(pid),CurrLeafPage(lp),index_in_page(index)
{
     
}
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() 
{
    if(!CurrLeafPage)//Current leaf page is not empty.
    {
     buff_pool_manager->UnpinPage(CurrLeafPage->GetPageId(),false);
    }
   //delete buff_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  
  return CurrLeafPage->GetItem(index_in_page);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() 
//Add ,and then return
{
   if (index_in_page>=CurrLeafPage->GetSize()-1){//Last Element
    page_id_t nextid =CurrLeafPage->GetNextPageId();
    if (nextid!=INVALID_PAGE_ID){
         //Fetch the next page
      Page* p = buff_pool_manager->FetchPage(nextid);
      index_in_page=0;
      CurrLeafPage = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *>(p->GetData());
      CurrPageID=CurrLeafPage->GetPageId();

    }
    else{
  
      index_in_page++;
    }
  }
  else{
    index_in_page ++;
  }
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
 if(this->CurrLeafPage->GetPageId()==itr.CurrLeafPage->GetPageId() &&  this->GetIndexInPage()==itr.GetIndexInPage()) 
 {
      return true;
 }
   return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
 if(this->CurrLeafPage->GetPageId()==itr.CurrLeafPage->GetPageId()  &&  this->GetIndexInPage()==itr.GetIndexInPage()) 
 {
      return false;
 }
   return true;
//return !operator==(itr);
}
INDEX_TEMPLATE_ARGUMENTS
page_id_t INDEXITERATOR_TYPE::GetCurrPageID()const
{
     return CurrPageID;
}
INDEX_TEMPLATE_ARGUMENTS
  void INDEXITERATOR_TYPE::SetCurrPageID(page_id_t page_id)
  {
      this->CurrPageID=page_id;
  }
  INDEX_TEMPLATE_ARGUMENTS
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* INDEXITERATOR_TYPE:: GetLeafPage()const
  {
        return CurrLeafPage;
  }
  INDEX_TEMPLATE_ARGUMENTS
  void INDEXITERATOR_TYPE::SetLeafPage( BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*leafpage)
  {
     CurrLeafPage=leafpage;
  }
  INDEX_TEMPLATE_ARGUMENTS
  int INDEXITERATOR_TYPE::GetIndexInPage()const
  {
       return index_in_page;
  }
  INDEX_TEMPLATE_ARGUMENTS
  void INDEXITERATOR_TYPE::SetIndexInPage(int index)
  {
       index_in_page=index;
  }



template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
