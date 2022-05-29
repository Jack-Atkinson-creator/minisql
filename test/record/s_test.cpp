#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"
#include "record/column.h"
char *chars[] = {
        const_cast<char *>(""),
        const_cast<char *>("hello"),
        const_cast<char *>("world!"),
        const_cast<char *>("\0")
};

Field int_fields[] = {
        Field(TypeId::kTypeInt, 188),
        Field(TypeId::kTypeInt, -65537),
        Field(TypeId::kTypeInt, 33389),
        Field(TypeId::kTypeInt, 0),
        Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
        Field(TypeId::kTypeFloat, -2.33f),
        Field(TypeId::kTypeFloat, 19.99f),
        Field(TypeId::kTypeFloat, 999999.9995f),
        Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {
        Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
        Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
        Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
        Field(TypeId::kTypeChar, chars[3], 1, false)
};
Field null_fields[] = {
        Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)
};

TEST(TupleTest, RowTest) {
  SimpleMemHeap heap;
//   TablePage table_page;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  std::vector<Field> fields = {
        Field(TypeId::kTypeInt, 188),
          Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
          Field(TypeId::kTypeFloat, 19.99f)
  };
  std::vector<Field> fields2 = {
        Field(TypeId::kTypeInt, 100),
          Field(TypeId::kTypeChar, const_cast<char *>("minisqlxxx"), strlen("minisqlxxx"), false),
          Field(TypeId::kTypeFloat, 59.99f)
  };
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  char buf[200],buf2[200];
  std::vector<Column *> col2;
  Schema sch2(col2);
  Schema* ptr2sch = &sch2;
  uint32_t k = schema->SerializeTo(buf);
  cout<<k<<endl;
  for(int i=0;i<int(k);i++)
        printf("%02x",buf[i]);
  cout<<endl;
  k = sch2.DeserializeFrom(buf, ptr2sch, &heap);
  cout<<k<<endl;
  k = ptr2sch->SerializeTo(buf2);
  cout<<k<<endl;
  for(int i=0;i<int(k);i++)
        printf("%02x",buf2[i]);
  cout<<endl;
  
//   uint32_t t = row.SerializeTo(buf,&*schema);
//   for(int i=0;i<int(t);i++)cout<<buf[i];
//   cout<<" "<<t;
//   cout<<endl;
//   Row row2(fields2);
//   t = row2.GetSerializedSize(&*schema); 
//   row2.DeserializeFrom(buf, &*schema);
//   cout<<t<<endl;
//   t = row2.GetSerializedSize(&*schema);
//   cout<<t<<endl;
//   EXPECT_EQ(CmpBool::kFalse, fields2[0].CompareEquals(*row2.GetField(0)));
//   EXPECT_EQ(CmpBool::kTrue, fields[0].CompareEquals(*row2.GetField(0)));
}