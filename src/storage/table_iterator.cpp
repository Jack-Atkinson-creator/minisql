#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {

}


TableIterator::TableIterator(TableHeap *_table_heap, RowId _rid)
    : table_heap(_table_heap),row(new Row(_rid)){
      //Row row_(_rid);
  if (row->GetRowId().GetPageId() != INVALID_PAGE_ID) {
    table_heap->GetTuple(row,nullptr);
    //row=&row_;
  }
}

TableIterator::TableIterator(const TableIterator &other):
table_heap(other.table_heap),row(new Row(*other.row))
{
  
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row->GetRowId()==itr.row->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

Row &TableIterator::operator*() {
  //ASSERT(false, "Not implemented yet.");
  return *(this->row);
}

TableIterator& TableIterator::operator=(const TableIterator &other){
  table_heap=other.table_heap;
  row=other.row;
  return *this;
}

Row *TableIterator::operator->() {
  ASSERT(*this != table_heap->End(),"itr is at end");
  return row;
}

TableIterator &TableIterator::operator++() {
  
  BufferPoolManager *buffer_pool_manager=table_heap->buffer_pool_manager_;//获得缓冲池管理权限
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager->FetchPage(row->GetRowId().GetPageId())); 
  ASSERT(page!=nullptr,"Can't have empty page!");
  //获得当前record所在页
  RowId NextId;//得到下一条record的rid
  bool isGet;
  isGet=page->GetNextTupleRid(row->GetRowId(),&NextId);
  //Row Nextrow(NextId);
  if(!isGet){//找不到下一条记录
    //当前记录为一页的最后一条记录S
    while(page->GetNextPageId()!=INVALID_PAGE_ID){
      auto next_page=static_cast<TablePage *>(buffer_pool_manager->FetchPage(page->GetNextPageId()));
      buffer_pool_manager->UnpinPage(page->GetTablePageId(),false);//写回
      page=next_page;
      if(page->GetFirstTupleRid(&NextId)){
        break;//找到next了
      }
    }
  }
  delete row;//删掉旧的
  row= new Row(NextId);//新建一个新的，否则会一直累加
  if(*this!=table_heap->End()){//不是末尾
    Transaction *txn=nullptr;
    table_heap->GetTuple(row,txn);
  }

  buffer_pool_manager->UnpinPage(page->GetTablePageId(),false);
  //没有修改，不是脏页
  return *this;
  
 //return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator old_heap(*this);
  ++(*this);
  return old_heap;
  //return TableIterator();
}
