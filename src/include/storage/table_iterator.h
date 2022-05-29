#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  TableIterator();
  
  TableIterator(TableHeap *_table_heap, RowId _rid);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;
  Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

  TableIterator& operator=(const TableIterator &other);

private:
  // add your own private member variables here
  TableHeap *table_heap;//table堆的指针，访问一个文件堆
  Row *row;//指向记录
};

#endif //MINISQL_TABLE_ITERATOR_H