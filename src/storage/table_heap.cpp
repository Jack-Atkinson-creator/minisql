#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  uint32_t record_len = row.GetSerializedSize(schema_); //得到row序列化的长度
  if(record_len>TablePage::SIZE_MAX_ROW){
    //极端情况下，只放一条记录（文件头+该记录偏移量+记录长度+记录），也放不下
    return false;
  }
    //把第一个page读出来
  TablePage *NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),false);
  while(1){//循环找到最近的能放的page
    if(NowPage->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
      buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);//设置为脏页
      return true;//成功插入
    }
    page_id_t NextPageId = NowPage->GetNextPageId();
    if (NextPageId == INVALID_PAGE_ID){
      break;
    }
    NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(NextPageId));
    buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),false);
  }
  int new_page_id = INVALID_PAGE_ID;
  buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);//设置为脏页
  TablePage *New_Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  buffer_pool_manager_->UnpinPage(New_Page->GetPageId(),false);//解锁
  if (New_Page==nullptr) return false;
  //完成双向连接
  New_Page->Init(new_page_id,NowPage->GetPageId(),log_manager_,txn);
  New_Page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
  buffer_pool_manager_->UnpinPage(New_Page->GetPageId(),true);//设置为脏页
  NowPage->SetNextPageId(new_page_id);
  buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);//设置为脏页    
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  //先找到旧的记录所在页
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page==nullptr){//找不到这个记录
    return false;
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  //找的到
  Row old_row(rid);  //把原来的row存下来,方便rollback
  bool Isupdate;//保存子函数结果
  Isupdate=page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(Isupdate){//成功更新
    //RowId new_row_id (rid.GetPageId(),row.GetRowId().GetSlotNum());
    row.SetRowId(rid);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    ASSERT(row.GetRowId().GetPageId()!=INVALID_PAGE_ID, "cuocuocuo in page!");
    return true;
  }else{//更新失败
    uint32_t serialized_size = row.GetSerializedSize(schema_);
    ASSERT(serialized_size > 0, "Can not have empty row.");
    uint32_t slot_num = old_row.GetRowId().GetSlotNum();
    uint32_t tuple_size = page->GetTupleSize(slot_num);
    if(slot_num>=page->GetTupleCount() || TablePage::IsDeleted(tuple_size)){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      return false;
    }else{//因为空间不足导致插入失败
      //page_id_t next_page_id=page->GetNextPageId();
      //auto next_page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      bool isInsert;
      isInsert=InsertTuple(row,txn);//判断有没有成功插入
      ASSERT(row.GetRowId().GetPageId()!=INVALID_PAGE_ID, "cuocuocuo OutPage!!!");
      //当前无法插入可以直接调用插入函数插到别的地方，但同时rid会被修改，所以更改了参数类型
      if(isInsert)
        MarkDelete(rid,txn);//将旧记录处标记为删除
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), isInsert);
      return isInsert;
    }
  }

  //return false;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page!=nullptr);
  page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  //把第一个page读出来
  TablePage *NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  while(1){//循环找到最近的能放的page
    buffer_pool_manager_->UnpinPage(NowPage->GetPageId(),true);//设置为脏页
    page_id_t NextPageId = NowPage->GetNextPageId();//找到下一页
    buffer_pool_manager_->DeletePage(NowPage->GetPageId());//删到这一页
    if (NextPageId == INVALID_PAGE_ID){
      return;
    }
    NowPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(NextPageId));
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page==nullptr){
    return false;
  }else{
    bool isGet;
    isGet=page->GetTuple(row,schema_,txn,lock_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return isGet;
  }
  //return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  //找到第一条记录，创建一个迭代器，把row给它
  //return TableIterator();
  RowId FirstId;
  auto page_id=first_page_id_;
  while(page_id!=INVALID_PAGE_ID){
    auto page=static_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    auto isFound=page->GetFirstTupleRid(&FirstId);
    buffer_pool_manager_->UnpinPage(page_id,false);
    if(isFound){
      break;
    }
    page_id=page->GetNextPageId();
  }
  return TableIterator(this,FirstId);
}

TableIterator TableHeap::End() {
  //return TableIterator();
  return TableIterator(this,RowId(INVALID_ROWID));
}

