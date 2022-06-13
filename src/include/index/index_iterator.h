#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(BufferPoolManager*b,page_id_t pid,BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*lp,int index);

  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

  page_id_t GetCurrPageID()const;
  void SetCurrPageID(page_id_t page_id);
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*GetLeafPage()const;
  void SetLeafPage( BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*leafpage);
  int GetIndexInPage()const;
  void SetIndexInPage(int index);

private:
  // add your own private member variables here
  BufferPoolManager*buff_pool_manager;
  page_id_t CurrPageID; //Current page_id we visit
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*CurrLeafPage;//Current leaf page
  int index_in_page;
};


#endif //MINISQL_INDEX_ITERATOR_H
