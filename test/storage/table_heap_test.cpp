#include <vector>
#include <unordered_map>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::vector<RowId>row_id;
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  //测试插入
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields = new Fields{
            Field(TypeId::kTypeInt, i),
            Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
    };
    Row row(*fields);
    ASSERT_EQ(true,table_heap->InsertTuple(row, nullptr));
    row_values[row.GetRowId().Get()] = fields;
    row_id.push_back(row.GetRowId());
    delete[] characters;
  }
  ASSERT_EQ(row_nums, row_values.size());
  //测试Get
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    ASSERT_EQ(true,table_heap->GetTuple(&row, nullptr));
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
  }
  //迭代器测试
  for (auto iter=table_heap->Begin(nullptr) ; iter != table_heap->End(); iter++) {
    Row &it_row = *iter;
    ASSERT_EQ(schema.get()->GetColumnCount(), it_row.GetFields().size());
    //加入迭代器的测试
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, it_row.GetField(j)->CompareEquals(row_values[it_row.GetRowId().Get()]->at(j)));
    }
    //delete row_kv.second;
    //free spaces
    delete row_values[it_row.GetRowId().Get()];
  }

  //update&delete测试
  std::unordered_map<int64_t, Fields *> row_values2;
  std::set<page_id_t>Used_Page;
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields = new Fields{
            Field(TypeId::kTypeInt, i),
            Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
    };
    Row row(*fields);
    ASSERT_EQ(true,table_heap->UpdateTuple(row,row_id.at(i),nullptr));
    ASSERT_EQ(false,row.GetRowId().GetPageId()==INVALID_PAGE_ID);
    row_values2[row.GetRowId().Get()] = fields;
    delete[] characters;
    
  }
  for (auto row_kv2 : row_values2) {
    Row row(RowId(row_kv2.first));
    ASSERT_EQ(true,table_heap->GetTuple(&row, nullptr));
    Used_Page.insert(row.GetRowId().GetPageId());
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv2.second->at(j)));
    }
    //测试delete
    //确定update没问题后，把记录删除
    table_heap->ApplyDelete(row.GetRowId(),nullptr);
    ASSERT_FALSE(table_heap->GetTuple(&row,nullptr));
  }
  //测试free
  table_heap->FreeHeap();
  auto *disk_manager = new DiskManager(db_file_name);
  auto *bpm = new BufferPoolManager(100, disk_manager);
  ASSERT_EQ(true,bpm->IsPageFree(*Used_Page.begin()));
  for ( auto page_iter = Used_Page.begin() ; page_iter!=Used_Page.end();page_iter++){
    ASSERT_EQ(true,disk_manager->IsPageFree(*page_iter));
  }
}

