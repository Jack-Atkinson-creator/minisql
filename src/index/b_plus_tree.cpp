#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
            
            root_page_id_=INVALID_PAGE_ID;//Initialize :Empty.
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() 
{
     buffer_pool_manager_=NULL;
     leaf_max_size_=internal_max_size_=0;
     root_page_id_=INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
   //root is invalid ---->Empty.
  return root_page_id_==INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) 
//Transaction:Unused.
{
  //Leaf page that POSSIBLY contains key.
  Page*P1=FindLeafPage(key);
  BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*bpt_lp= reinterpret_cast< BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(P1->GetData());
  ValueType vi;
  bool ans= bpt_lp->Lookup(key,vi,comparator_);//Test if key exists.
  if(ans) //Can find the result.
   {
      result.push_back(vi);
   }
  buffer_pool_manager_->UnpinPage(P1->GetPageId(),false);

  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
   if(IsEmpty())//Empty Tree
   {
        StartNewTree(key,value);
        InsertIntoLeaf(key,value,transaction);
        return true;
   }
   else{//Non-empty;
    return InsertIntoLeaf(key, value, transaction);
   }
  
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value)
{
  //Fetch a new page.
    page_id_t NewID;
    Page*root=buffer_pool_manager_->NewPage(NewID);
    BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*ROOT=reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(root->GetData());
    ROOT->Init(NewID,INVALID_PAGE_ID,leaf_max_size_);
    //Update root_id.
    root_page_id_=NewID;   
    buffer_pool_manager_->UnpinPage(NewID,false);
    UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
    Page*p1=FindLeafPage(key);
    BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*leafNode = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>*>(p1->GetData());
    ValueType a;
    bool isExist = leafNode->Lookup(key, a, comparator_);//Check "key" is already exists.
    if (isExist) {
      //throw("duplicate keys.")
        buffer_pool_manager_->UnpinPage(leafNode->GetPageId(), false);
        return false;
    }

    if (leafNode->GetSize() < leafNode->GetMaxSize()) //The leaf can insert things and doesn't split.
    {
        leafNode->Insert(key, value, comparator_);
        buffer_pool_manager_->UnpinPage(leafNode->GetPageId(), true);
    } else { //leafsize=maxsize

        leafNode->Insert(key, value, comparator_);
        //Split into 2 nodes. New_Leaf is bigger in keys.
        BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>* Leaf2 = Split(leafNode);
        InsertIntoParent(leafNode, Leaf2->KeyAt(0), Leaf2, transaction);//Unpin
    }
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

/*
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  
}
*/
  INDEX_TEMPLATE_ARGUMENTS
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* BPLUSTREE_TYPE::Split(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*node)
  {
    page_id_t NewID;
   Page *p=buffer_pool_manager_->NewPage(NewID);
   InternalPage*newnode=reinterpret_cast<InternalPage*>(p->GetData());
   newnode->Init(NewID,node->GetParentPageId(),internal_max_size_);
    node->MoveHalfTo(newnode,buffer_pool_manager_);
  return newnode;
  }

  INDEX_TEMPLATE_ARGUMENTS
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* BPLUSTREE_TYPE::Split(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*node)
 {
    page_id_t NewID;
   Page *p=buffer_pool_manager_->NewPage(NewID);
   LeafPage*newnode=reinterpret_cast<LeafPage*>(p->GetData());
   newnode->Init(NewID,node->GetParentPageId(),leaf_max_size_);
    node->MoveHalfTo(newnode);
  return newnode;
 }

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) 
{
  if(old_node->IsRootPage())
    //Create a new root
   {
     page_id_t NewID;
     Page *p=buffer_pool_manager_->NewPage(NewID);
    BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*Root_Newly=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p->GetData());
      Root_Newly->Init(NewID,INVALID_PAGE_ID,internal_max_size_);
      Root_Newly->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
      new_node->SetParentPageId(Root_Newly->GetPageId());
      old_node->SetParentPageId(Root_Newly->GetPageId());

      //Update header_page
      this->root_page_id_=NewID;
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(Root_Newly->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
      return ;
   }
   else  //Move the key into the parent.
   {
        new_node->SetParentPageId(old_node->GetParentPageId());
        //Fetch the parent page.
        page_id_t ParID=old_node->GetParentPageId();
        Page*p=buffer_pool_manager_->FetchPage(ParID);
        BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*par_page=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p->GetData());
       if(par_page->GetMaxSize()>par_page->GetSize())
       //parent_page is not full.Insert directly.
       {
           par_page->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
             //Unpin all the used pages.
             buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
           buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true); 
           buffer_pool_manager_->UnpinPage(ParID,true);
           return;
       }
       else
       //Parent needs to be split.
       {
             par_page->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
             BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*new_internal=Split(par_page);
             //Move upwards to the grandparent.
             InsertIntoParent(par_page,new_internal->KeyAt(0),new_internal,transaction);
           buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(ParID, true);
       }
   }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) 
{
    Page*p1=FindLeafPage(key);
    if(p1==NULL)//Could not find.
        return;
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*Page_To_Del = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(p1->GetData());
  
    page_id_t del_id = Page_To_Del->GetPageId();
    int SS = Page_To_Del->RemoveAndDeleteRecord(key, comparator_);//size after deletion.
    if(SS>=Page_To_Del->GetMinSize())//Delete directly
    {
          buffer_pool_manager_->UnpinPage(del_id, true);
    }
    else //need to Redistribute or Merge 
        CoalesceOrRedistribute(Page_To_Del, transaction);
    return;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)
{
   assert(node->GetMinSize() > node->GetSize());//node must be lower than Min_Size.
    if (node->IsRootPage()) {
        //Root 
        return AdjustRoot(node);
    }
     N* sib= NULL;//node's sibling     
     //Find node's parent.
   Page *p1 = buffer_pool_manager_->FetchPage(node->GetParentPageId());
   BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*par_page=reinterpret_cast< BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p1->GetData());
    int i_th_child = par_page->ValueIndex(node->GetPageId());
    bool isLeftSib=false;
    Page *sib_page=NULL;
    if(i_th_child==0)
      {
        sib_page =buffer_pool_manager_->FetchPage(par_page->ValueAt(i_th_child+1));
        //right sib
      }
      else{
        sib_page=buffer_pool_manager_->FetchPage(par_page->ValueAt(i_th_child-1));
        isLeftSib=true;//left sib
      }
    sib = reinterpret_cast<N *>(sib_page->GetData());//node's sibling
    buffer_pool_manager_->UnpinPage(par_page->GetPageId(), false);//Unpin the parent.
   
   Page* p2 = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    par_page = reinterpret_cast< BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(p2->GetData());

    if (node->GetSize() + sib->GetSize() <= node->GetMaxSize()) {
      //Merge ,the leaf page should be deleted.
        Coalesce(&sib,&node,&par_page,i_th_child,isLeftSib,transaction);
        buffer_pool_manager_->UnpinPage(par_page->GetPageId(), true);
        return true;
    } else {//borrow from siblings.
       Redistribute(sib,node,i_th_child,isLeftSib);
    }
    buffer_pool_manager_->UnpinPage(par_page->GetPageId(), true);
    return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,bool isleft,
                              Transaction *transaction) 
  {
    page_id_t SibId = (*neighbor_node)->GetPageId();
    page_id_t NodeId = (*node)->GetPageId();
   KeyType midkey;
    if (isleft) { //neighbor is the left sibling of the node/      
        midkey= (*parent)->KeyAt(index);
        (*node)->MoveAllTo(*neighbor_node, midkey, buffer_pool_manager_);
        buffer_pool_manager_->UnpinPage(NodeId, true);
        buffer_pool_manager_->DeletePage(NodeId) ;//Delete "node" page.
        buffer_pool_manager_->UnpinPage(SibId, true);
        (*parent)->Remove(index);
    } else { //neighbor is the right sibling of the node.
        midkey= (*parent)->KeyAt(index+1);
        (*neighbor_node)->MoveAllTo(*node, midkey, buffer_pool_manager_);

        buffer_pool_manager_->UnpinPage(NodeId, true);
        buffer_pool_manager_->UnpinPage(SibId, true);
        buffer_pool_manager_->DeletePage(SibId);//Delete "neighbor_node" page.
        (*parent)->Remove(index + 1);
    }

   if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
       //After a parent node is deleted, the number of nodes is less than min_size, which is to process recursively.
        return CoalesceOrRedistribute(*parent, transaction);
    }
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index,bool isleft) 
{
  if(!isleft) //neighbor is a right sibling
  {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  }
   else{ //neighbor is a left sibling
      neighbor_node->MoveLastToFrontOf(node, buffer_pool_manager_);
   }
   buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  //Size(root)=0 or 1.
   if (old_root_node->IsLeafPage()&&old_root_node->GetSize()==0) //root is also leaf
   {
        root_page_id_ = INVALID_PAGE_ID;
        buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
        buffer_pool_manager_->DeletePage(old_root_node->GetPageId());//delete the root.
    
    }
    else{
    //assert(old_root_node->GetSize() == 1);
   BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* Last_Root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(old_root_node);
    root_page_id_ =Last_Root->ValueAt(0);
    UpdateRootPageId(0);
    //Fetch new root page
    Page *p1 = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *newroot = reinterpret_cast<BPlusTreePage *>(p1->GetData());
    newroot->SetParentPageId(INVALID_PAGE_ID);//New root --->set it into a root node.
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(newroot->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());//delete the root.
    }
   return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key;
  Page*begin=FindLeafPage(key,true);//Find the leftest leaf page.
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*beginPage=reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(begin->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_,beginPage->GetPageId(),beginPage,0);//Initial index=0;
}


/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
   Page*begin=FindLeafPage(key,false);//Find the first leaf page(>=key)
   BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*beginPage=reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(begin->GetData());
  int Begin=0,ind=0;
  if(beginPage!=nullptr)
  {
     ind=beginPage->KeyIndex(key,comparator_);
     if(beginPage->GetSize()>=1 && ind<beginPage->GetSize() &&comparator_(beginPage->GetItem(ind).first,key)==0)
     //Find the key in leaf.
     {
           Begin=ind;
     }
     else
     //There doesn't exist the key in leaf.
     {
          Begin=beginPage->GetSize();
     }

  }
    return INDEXITERATOR_TYPE(buffer_pool_manager_,beginPage->GetPageId(),beginPage,Begin);
     
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
   //Fetch the rightest leaf page.
  Page*currpage=buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage*bptp=reinterpret_cast<BPlusTreePage*>(currpage->GetData());
  BPlusTreePage*Unpin_BPTP=bptp;//B+Tree page that needs to be unpinned.
  page_id_t NextTurn;
  while(bptp->IsLeafPage()!=true)//non-leaf
  {
     BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*IntBPTP=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(bptp);
     NextTurn=IntBPTP->ValueAt(IntBPTP->GetSize()-1);//Rightest pointer.
     Unpin_BPTP=bptp;
     //Fetch the page
     currpage= buffer_pool_manager_->FetchPage(NextTurn);
      bptp=reinterpret_cast<BPlusTreePage*>(currpage->GetData());
      //Unpin the last page
      buffer_pool_manager_->UnpinPage(Unpin_BPTP->GetPageId(),false);
  }   
  //bptp is a leaf
  int end_index=bptp->GetSize();
  buffer_pool_manager_->UnpinPage(bptp->GetPageId(),false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_,INVALID_PAGE_ID,nullptr,end_index);
}


/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
   //Fetch the page of root_id
  Page*currpage=buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage*bptp=reinterpret_cast<BPlusTreePage*>(currpage->GetData());
  BPlusTreePage*Unpin_BPTP=bptp;//B+Tree page that needs to be unpinned.
  page_id_t NextTurn;
  while(bptp->IsLeafPage()!=true)//non-leaf
  {
     BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*IntBPTP=reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(bptp);
   
     if(leftMost)
     {
           NextTurn=IntBPTP->ValueAt(0);
     }else
     {
       NextTurn = IntBPTP->Lookup(key,comparator_);
     } 
     Unpin_BPTP=bptp;
     //Fetch the page
     currpage= buffer_pool_manager_->FetchPage(NextTurn);
      bptp=reinterpret_cast<BPlusTreePage*>(currpage->GetData());
      //Unpin the last page
      buffer_pool_manager_->UnpinPage(Unpin_BPTP->GetPageId(),false);
  }   
  //Currpage is leaf ,return
  return currpage;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) 
{
  /*
    IndexRootsPage*root_page=reinterpret_cast<IndexRootsPage*>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    if(insert_record==0)//update in index_roots_page
    {
        root_page->Update(index_id_,root_page_id_);
    }
    else{ //insert into index_roots_page
         root_page->Insert(index_id_,root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);*/
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
