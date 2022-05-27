#include "executor/execute_engine.h"
#include "glog/logging.h"

#include <iostream>
#include <fstream>
#include <cstdio>
#include <string>
#include <iomanip>
#include <map>
#include <cstdlib>
#include <algorithm>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
#include <direct.h> 
#else
#include <dirent.h>
#include <unistd.h>
#endif

extern "C" {
    int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
using namespace std;

/// <summary>
/// 遍历目录中的db文件，然后制作map
/// </summary>
/// <param name="folderPath"></param>
void ExecuteEngine::DBIntialize()
{
#ifdef _WIN32
    long long hdl = 0;
    struct _finddata_t fileinfo;
    string str;
    if ((hdl = _findfirst(str.assign(dbPath).append("\\*.db").c_str(), &fileinfo)) != -1)
    {
        do
        {
            if ((fileinfo.attrib & _A_SUBDIR))  //判断是否为文件夹
            {
                continue;
            }
            else    //文件处理
            {
                str.assign(dbPath).append("/").append(fileinfo.name); //路径

                DBStorageEngine* dbse = new DBStorageEngine(str, false);
                dbs_.insert(pair<string, DBStorageEngine*>(fileinfo.name, dbse));

            }
        } while (_findnext(hdl, &fileinfo) == 0);  //寻找下一个，成功返回0，否则-1
        _findclose(hdl);
    }
#else
    DIR* dir;
	
    if ((dir = opendir(dbPath.c_str())) == 0)	//无法打开则跳过
    {
        return;
    }

    struct dirent* stdir;

    while (true)
    {
        if ((stdir = readdir(dir)) == 0)
        {
            break;
        }
        if (stdir->d_type == 8)
        {
            string fileName = stdir->d_name;
            string ext(".db");
            if (fileName.compare(fileName.size() - ext.size(), ext.size(), ext) == 0)
            {
                DBStorageEngine* dbse = new DBStorageEngine(dbPath + "/" + fileName, false);
                dbs_.insert(pair<string, DBStorageEngine*>(fileName, dbse));
            }
        }
    }
    closedir(dir);		//关闭目录
#endif
}


ExecuteEngine::ExecuteEngine() 
{
    current_db_ = "";
    curDB = nullptr;
    char buffer[1024];
    getcwd(buffer, 1024); //当前路径
    dbPath = buffer;

    //初始化执行器，读取文件装进map里
    DBIntialize();
}


dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}


/// <summary>
/// Create数据库，直接新建一个对象然后插入map中
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  if (ast->child_->type_ == kNodeIdentifier)
    {
        string dbName = ast->child_->val_;
        dbName += ".db";

        if (dbs_.find(dbName) == dbs_.end())
        {
            //没找到
            DBStorageEngine* dbse = new DBStorageEngine(dbPath + "/" + dbName);
            dbs_.insert(pair<string, DBStorageEngine*>(dbName, dbse));

            cout << "minisql: Create Successfully.\n";
            return DB_SUCCESS;
        }
        else {
            //找到了，报错
            cout << "minisql[ERROR]: Database existed.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
}


/// <summary>
/// delete指令，删除数据库
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  if (ast->child_->type_ == kNodeIdentifier)
    {
        string dbName = ast->child_->val_; //要删除的数据库名字
        string fileName = dbName + ".db";
        unordered_map<string, DBStorageEngine*>::iterator it = dbs_.find(fileName);
        if (it != dbs_.end())
        {
            //找到了
            if (remove((dbPath + "/" + dbName).c_str()) == 0)
            {
                //删除成功
                cout << "minisql: Delete successfully.\n";
                return DB_FAILED;
            }
            else {
                //报错
                cout << "minisql[ERROR]: Delete failed.\n";
                return DB_FAILED;
            }
            //map中删除
            delete it->second;
            dbs_.erase(it);
        }
        else {
            //没找到，报错
            cout << "minisql[ERROR]: No database.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
}


/// <summary>
/// 直接把map里面的列出来就行
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  size_t dbNum = dbs_.size();

    cout << "+--------------------+\n";
    cout << "| Database           |\n";
    cout << "+--------------------+\n";

    if (dbNum != 0)
    {
        for (auto it : dbs_) 
        {
            cout << "| " << setw(19) << it.first << "|" << endl;
        }
        cout << "+--------------------+\n";
    }

    cout << "The number of databases: " << dbNum << endl;
    return DB_SUCCESS;
}


/// <summary>
/// 切换当前数据库
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  if (ast->child_->type_ == kNodeIdentifier)
    {
        string dbName = ast->child_->val_; //要切换的数据库名字
        string fileName = dbName + ".db";

        unordered_map<string, DBStorageEngine*>::iterator it = dbs_.find(fileName);
        if (it != dbs_.end())
        {
            //找到了
            current_db_ = it->first;
            curDB = it->second;
            cout << "minisql: Database changed.\n";
            return DB_SUCCESS;
        }
        else {
            //没找到
            cout << "minisql[ERROR]: No database.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
}



/// <summary>
/// 列出数据库下的所有表
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_ != "")
    {
        vector<TableInfo*> tables;
        if (curDB->catalog_mgr_->GetTables(tables) == DB_SUCCESS)
        {
            int i = 0;
            for (auto it : tables)
            {
                if (i != 0)
                {
                    cout << endl << it->GetTableName();
                }
                else {
                    cout << it->GetTableName();
                }
            }
            cout << endl << "The number of tables: " << tables.size() << endl;
            return DB_SUCCESS;
        }
        else {
            cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
}


/// <summary>
/// 创建table
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_ != "")
    {
        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        string tableName = ast->child_->val_;

        //不能跟已有的重名
        TableInfo* testInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, testInfo) == DB_SUCCESS) //找到这个名字了，报错
        {
            if (testInfo->GetTableName() == tableName)
            {
                cout << "minisql: Table existed.\n";
                return DB_FAILED;
            }
        }



        pSyntaxNode defList = ast->child_->next_;
        if (defList->child_ != nullptr)
        {
            vector<Column*> columns;

            pSyntaxNode colDef;
            uint32_t curIndex = 0;
            map<string, uint32_t> indexMap; //找主键用
            for (colDef = defList->child_; colDef->type_ != kNodeColumnDefinitionList && colDef != nullptr; colDef = colDef->next_, curIndex++)
            {
                string name = colDef->child_->val_;
                TypeId type = kTypeInvalid;
                uint32_t index = curIndex;
                bool nullable = true;
                bool unique = false;
                indexMap.insert(pair<string, uint32_t>(name, index));

                string colVal = colDef->val_;
                if (colVal == "unique")
                {
                    unique = true;
                }

                string typeName = colDef->child_->next_->val_;
                if (typeName == "int")
                {
                    type = kTypeInt;

                    Column* column = new Column(name, type, index, nullable, unique);
                    columns.push_back(column);
                }
                else if (typeName == "float")
                {
                    type = kTypeFloat;
                    Column* column = new Column(name, type, index, nullable, unique);
                    columns.push_back(column);
                }
                else {
                    type = kTypeChar;

                    //检验长度
                    string str = colDef->child_->next_->child_->val_;

                    if (str.find('.') == str.npos && str.find('-') == str.npos)
                    {
                        uint32_t length = atoi(str.c_str());

                        Column* column = new Column(name, type, length, index, nullable, unique);
                        columns.push_back(column);
                    }
                    else {
                        cout << "minisql: Input error.\n";
                        return DB_FAILED;
                    }
                }
             
            }

            //主键处理
            if (colDef != nullptr)
            {
                if (colDef->type_ == kNodeColumnDefinitionList)
                {
                    int* nullArray = new int[curIndex]();

                    for (pSyntaxNode pkNode = colDef->child_; pkNode != nullptr; pkNode = pkNode->next_)
                    {
                        string pkName = pkNode->val_; //主键名字

                        auto it = indexMap.find(pkName);
                        if (it != indexMap.end())
                        {
                            nullArray[it->second] = 1;
                        }
                        else {
                            cout << "minisql: Input error.\n";
                            return DB_FAILED;
                        }
                    }

                    for (int i = 0; i < curIndex; i++)
                    {
                        if (nullArray[i] != 1)
                        {
                            //不是主键
                            columns[i]->SetNullable(true);
                        }
                        else {
                            //是主键
                            columns[i]->SetNullable(false);
                        }
                    }
                }
            }


            //创建table，组装
            TableSchema* schema = new TableSchema(columns);
            if (curDB->catalog_mgr_->CreateTable(tableName, schema, nullptr, tableInfo) == DB_SUCCESS)
            {
                cout << "minisql: Create table successfully.\n";
                return DB_SUCCESS;
            }
            else {
                cout << "minisql: Failed.\n";
                return DB_FAILED;
            }
        }
        else {
            cout << "minisql: Input error.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
}


/// <summary>
/// 删除table
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (ast->child_->type_ == kNodeIdentifier)
    {
        if (current_db_ != "")
        {
            string tableName = ast->child_->val_;
            if (curDB->catalog_mgr_->DropTable(tableName) == DB_SUCCESS)
            {
                cout << "minisql: Delete successfully.\n";
                return DB_SUCCESS;
            }
            else {
                cout << "minisql[ERROR]: No table.\n";
                return DB_FAILED;
            }
        }
        else {
            cout << "minisql: No database selected.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
}


/// <summary>
/// 显示某个数据库下的所有index
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_ != "")
    {
        //先获得所有table
        vector<TableInfo*> tables;
        if (curDB->catalog_mgr_->GetTables(tables) == DB_SUCCESS)
        {
            int i = 0;
            for (auto tb : tables)
            {
                vector<IndexInfo*> indexes;
                if (curDB->catalog_mgr_->GetTableIndexes(tb->GetTableName(), indexes) == DB_SUCCESS)
                {
                    for (auto it : indexes)
                    {
                        if (i != 0)
                        {
                            cout << endl << tb->GetTableName() << ": " << it->GetIndexName();
                        }
                        else {
                            cout << tb->GetTableName() << ": " << it->GetIndexName();
                        }
                    }
                    cout << endl << "The number of indexes: " << indexes.size() << endl;
                    return DB_SUCCESS;
                }
                else {
                    cout << "minisql[ERROR]: Failed.\n";
                    return DB_FAILED;
                }
            }
        }
        else {
            cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
}




dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  return DB_FAILED;
}


/// <summary>
/// 删除索引，不能指定特定的表（受解释器限制）
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (ast->child_->type_ == kNodeIdentifier)
    {
        string indexName = ast->child_->val_;
        //先找出来索引对应的table
        vector<TableInfo*> tables;
        if (curDB->catalog_mgr_->GetTables(tables) == DB_SUCCESS)
        {
            for (auto it : tables)
            {
                //语句不支持输入table的名字，只能一个一个试
                if (curDB->catalog_mgr_->DropIndex(it->GetTableName(), indexName) == DB_SUCCESS)
                {
                    cout << "minisql: Delete index " << indexName << "." << endl;
                    return DB_SUCCESS;
                }
            }
            cout << "minisql: No index.\n";
            return DB_FAILED;
        }
        else {
            cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
}


/// <summary>
/// where子句分析
/// </summary>
/// <param name="valMap"></param>
/// <param name="typeMap"></param>
/// <param name="kNode"></param>
/// <returns></returns>
bool ExecuteEngine::ClauseAnalysis(map<string, Field*>& valMap, map<string, TypeId>& typeMap, pSyntaxNode kNode)
{
    if (kNode->child_ != nullptr && kNode->child_->next_ != nullptr)
    {
        string val = kNode->val_;
        string name = ""; //仅在比较运算里用
        switch (kNode->type_)
        {
        case kNodeCompareOperator:
            name = kNode->child_->val_;
            if (val == "is")
            {
                auto it = valMap.find(name);
                if (it != valMap.end())
                {
                    if (it->second->IsNull())
                    {
                        return true;
                    }
                    else {
                        return false;
                    }
                }
                else {
                    cout << "MiniSql: Input error\n";
                    return false;
                }        
            }
            else if (val == "not")
            {
                auto it = valMap.find(name);
                if (it != valMap.end())
                {
                    if (it->second->IsNull())
                    {
                        return false;
                    }
                    else {
                        return true;
                    }
                }
                else {
                    cout << "MiniSql: Input error\n";
                    return false;
                }
            }
            else {
                auto typeIt = typeMap.find(name);
                auto valIt = valMap.find(name);
                if (typeIt != typeMap.end() && valIt != valMap.end())
                {
                    TypeId type = typeIt->second; //表中的类型
                    Field* dataField = valIt->second; //表中实际的field
                    string str = kNode->child_->next_->val_; //语法树的原始比较数据

                    if (kNode->child_->next_->type_ == kNodeNull)
                    {
                        cout << "MiniSql: Use is/not instead of \'=\' or \'<>\'.\n";
                        return false;
                    }

                    Field* inField;
                    //根据不同类型装载field
                    
                    switch (kNode->child_->next_->type_)
                    {
                    case kNodeNumber:
                        if (type == kTypeInt)
                        {
                            //原数据是整数，输入的数据不是整数报错
                            if (str.find('.') == str.npos)
                            {
                                int32_t num = atoi(str.c_str());
                                inField = new Field(type, num);
                            }
                            else {
                                cout << "minisql[ERROR]: Input error.\n";
                                return DB_FAILED;
                            }
                        }
                        else {
                            //原数据是小数，输入数据直接装进field，输入整数也当作小数
                            float num = atof(kNode->child_->next_->val_);
                            inField = new Field(type, num);
                        }
                        break;
                    case kNodeString:
                        inField = new Field(type, kNode->child_->next_->val_, uint32_t(str.size() + 1), true);
                        break;
                    default:
                        cout << "MiniSql: Failed.\n";
                        return false;
                        break;
                    }

                    //根据不同运算符进行运算
                    CmpBool res;
                    if (dataField->CheckComparable(*inField))
                    {
                        switch (val[0])
                        {
                        case '=':
                            res = dataField->CompareEquals(*inField);
                            if (res != kNull)
                            {
                                return res == kTrue ? true : false;
                            }
                            else {
                                cout << "MiniSql: Failed\n";
                                return false;
                            }
                            break;
                        case '<':
                            if (val == "<=")
                            {
                                res = dataField->CompareLessThanEquals(*inField);
                                if (res != kNull)
                                {
                                    return res == kTrue ? true : false;
                                }
                                else {
                                    cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else if (val == "<")
                            {
                                res = dataField->CompareLessThan(*inField);
                                if (res != kNull)
                                {
                                    return res == kTrue ? true : false;
                                }
                                else {
                                    cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else if (val == "<>")
                            {
                                res = dataField->CompareNotEquals(*inField);
                                if (res != kNull)
                                {
                                    return res == kTrue ? true : false;
                                }
                                else {
                                    cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else {
                                cout << "MiniSql: Failed\n";
                                return false;
                            }
                            break;
                        case '>':
                            if (val == ">=")
                            {
                                res = dataField->CompareGreaterThanEquals(*inField);
                                if (res != kNull)
                                {
                                    return res == kTrue ? true : false;
                                }
                                else {
                                    cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else if (val == ">")
                            {
                                res = dataField->CompareGreaterThan(*inField);
                                if (res != kNull)
                                {
                                    return res == kTrue ? true : false;
                                }
                                else {
                                    cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else {
                                cout << "MiniSql: Failed\n";
                                return false;
                            }
                            break;
                        default:
                            cout << "MiniSql: Failed\n";
                            return false;
                            break;
                        }

                    }
                    else {
                        cout << "MiniSql: Type error\n";
                        return false;
                    }
                }
                else {
                    cout << "MiniSql: Input error\n";
                    return false;
                }
            }
            break;
        case kNodeConnector:
            if (val == "and")
            {
                return ClauseAnalysis(valMap, typeMap, kNode->child_) && ClauseAnalysis(valMap, typeMap, kNode->child_->next_);
            }
            else if (val == "or")
            {
                return ClauseAnalysis(valMap, typeMap, kNode->child_) || ClauseAnalysis(valMap, typeMap, kNode->child_->next_);
            }
            else {
                cout << "Clause error\n";
                return false;
            }
            break;
        default:
            cout << "Clause error\n";
            return false;
            break;
        }
    }
    else {
        cout << "Clause error\n";
        return false;
    }
}


/// <summary>
/// 选择，索引没做
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  if (current_db_ != "")
    {
        pSyntaxNode printNode = ast->child_;
        string tableName = ast->child_->next_->val_;
        uint32_t selNum = 0;


        //没有索引，用table迭代器遍历
        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
        {
            if (tableInfo->GetTableName() == tableName)
            {
                vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息
                int* isPrint = new int[columns.size()](); //默认均不输出
                

                if (printNode->type_ != kNodeAllColumns)
                {
                    //看要输出哪几列
                    map<string, uint32_t> nameMap; //名字到index的map

                    for (auto col : columns)
                    {
                        nameMap.insert(pair<string, uint32_t>(col->GetName(), col->GetTableInd()));
                    }
                    for (auto pNode = printNode->child_; pNode != nullptr; pNode = pNode->next_)
                    {
                        string colName = pNode->val_; //列名
                        auto it = nameMap.find(colName);

                        if (it != nameMap.end())
                        {
                            isPrint[it->second] = 1;
                        }
                        else {
                            cout << "minisql: Input error.\n";
                            return DB_FAILED;
                        }
                    }
                }
                else {
                    memset(isPrint, 1, columns.size()); //全设为1
                }

                for (auto iter = tableInfo->GetTableHeap()->Begin(nullptr); iter != tableInfo->GetTableHeap()->End(); iter++)
                {
                    //遍历每一行
                    vector<Field*> fields = iter->GetFields();
                    RowId rowID = iter->GetRowId();
                    map<string, Field*> valMap; //用于寻找对应field的map
                    map<string, TypeId> typeMap; //用于寻找对应type的id

                    //判断field和column大小是否相等
                    if (columns.size() == fields.size())
                    {
                        for (uint32_t index = 0; index <= columns.size(); index++)
                        {
                            valMap.insert(pair<string, Field*>(columns[index]->GetName(), fields[index]));
                            typeMap.insert(pair<string, TypeId>(columns[index]->GetName(), columns[index]->GetType()));
                        }

                        //有没有子句问题
                        if (ast->child_->next_->next_ == nullptr) //没有子句
                        {
                            //没有子句， 直接输出
                            for (uint32_t index = 0; index < columns.size(); index++)
                            {
                                if (isPrint[index])
                                {
                                    Field* field = fields[index];
                                    selNum++;
                                    if (field->IsNull())
                                    {
                                        if (index == 0)
                                        {
                                            cout << "NULL";
                                        }
                                        else {
                                            cout << "\t" << "NULL";
                                        }
                                    }
                                    else {
                                        string data = field->GetData();
                                        if (index == 0)
                                        {
                                            cout << data;
                                        }
                                        else {
                                            cout << "\t" << data;
                                        }
                                    }
                                }
                            }
                            cout << endl;
                        }
                        else {
                            //子句判断
                            if (ClauseAnalysis(valMap, typeMap, ast->child_->next_->next_->child_))
                            {
                                //符合条件，可以输出
                                for (uint32_t index = 0; index < columns.size(); index++)
                                {
                                    if (isPrint[index])
                                    {
                                        Field* field = fields[index];
                                        selNum++;
                                        if (field->IsNull())
                                        {
                                            if (index == 0)
                                            {
                                                cout << "NULL";
                                            }
                                            else {
                                                cout << "\t" << "NULL";
                                            }
                                        }
                                        else {
                                            string data = field->GetData();
                                            if (index == 0)
                                            {
                                                cout << data;
                                            }
                                            else {
                                                cout << "\t" << data;
                                            }
                                        }
                                    }
                                }
                                cout << endl;
                            }
                        }
                    }
                    else {
                        cout << "minisql: Failed.\n";
                        return DB_FAILED;
                    }
                }

                cout << "minisql: " << "The number of selected rows: " << selNum << ".\n";
                return DB_SUCCESS;
            }
        }
        else {
            cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
}


/// <summary>
/// 插入记录
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if (current_db_ != "")
    {
        string tableName = ast->child_->val_;

        //先找table
        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
        {
            if (tableInfo->GetTableName() == tableName)
            {
                vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息


                vector<Field> fields;
                uint32_t index = 0;
                for (auto columnPt = ast->child_->next_->child_; columnPt != nullptr; columnPt = columnPt->next_, index++)
                {
                    if (index < columns.size())
                    {
                        Column* curCol = columns[index];
                        TypeId type = curCol->GetType();
                        string str = columnPt->val_;

                        uint32_t len = 0;
                        switch (columnPt->type_)
                        {
                        case kNodeNumber:
                            if (type == kTypeInt)
                            {
                                if (str.find('.') == str.npos)
                                {
                                    int32_t num = atoi(str.c_str());
                                    Field field(type, num);
                                    fields.push_back(field);
                                }
                                else {
                                    cout << "minisql[ERROR]: Input error.\n";
                                    return DB_FAILED;
                                }
                            }
                            else if (type == kTypeFloat)
                            {
                                float num = atof(str.c_str());
                                Field field(type, num);
                                fields.push_back(field);
                            }
                            else {
                                cout << "minisql[ERROR]: Syntax error.\n";
                                return DB_FAILED;
                            }
                            break;
                        case kNodeString:
                            len = curCol->GetLength();
                            if (str.size() <= len)
                            {
                                Field field(type, columnPt->val_, len, true);
                                fields.push_back(field);
                            }
                            else {
                                cout << "minisql[ERROR]: String too long.\n";
                                return DB_FAILED;
                            }
                            break;
                        case kNodeNull:
                            if (curCol->IsNullable())
                            {
                                Field field(type);
                                fields.push_back(field);
                            }
                            else {
                                cout << "minisql[ERROR]: " << curCol->GetName() << "can't be nullable." << endl;
                                return DB_FAILED;
                            }
                            break;
                        default:
                            cout << "minisql[ERROR]: Syntax error.\n";
                            return DB_FAILED;
                            break;
                        }
                    }
                    else {
                        cout << "minisql[ERROR]: Input error.\n";
                        return DB_FAILED;
                    }
                }
                if (index == columns.size()) //列数匹配，继续
                {
                    Row row(fields);

                    if (tableInfo->GetTableHeap()->InsertTuple(row, nullptr))
                    {
                        cout << "minisql: Insert successfully.\n";
                        return DB_SUCCESS;
                    }
                    else {
                        cout << "minisql[ERROR]: Insert failed.\n";
                        return DB_FAILED;
                    }
                }
                else {
                    cout << "minisql[ERROR]: Input error.\n";
                    return DB_FAILED;
                }
            }
        }
        else {
            cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
}

/// <summary>
/// 删除，索引没做
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  if (current_db_ != "")
    {
        string tableName = ast->child_->val_;
        uint32_t delNum = 0;

        //没有索引，用table迭代器遍历
        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
        {
            if (tableInfo->GetTableName() == tableName)
            {
                vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息

                for (auto iter = tableInfo->GetTableHeap()->Begin(nullptr); iter != tableInfo->GetTableHeap()->End(); iter++)
                {
                    //遍历每一行
                    vector<Field*> fields = iter->GetFields();
                    RowId rowID = iter->GetRowId();
                    map<string, Field*> valMap; //用于寻找对应field的map
                    map<string, TypeId> typeMap; //用于寻找对应type的id
                    
                    //判断field和column大小是否相等
                    if (columns.size() == fields.size())
                    {
                        for (uint32_t index = 0; index <= columns.size(); index++)
                        {
                            valMap.insert(pair<string, Field*>(columns[index]->GetName(), fields[index]));
                            typeMap.insert(pair<string, TypeId>(columns[index]->GetName(), columns[index]->GetType()));
                        }

                        //有没有子句问题
                        if (ast->child_->next_ == nullptr) //没有子句
                        {
                            //没有子句， 直接删
                            if (tableInfo->GetTableHeap()->MarkDelete(rowID, nullptr))
                            {
                                delNum++;
                            }
                            else {
                                cout << "minisql: Failed.\n";
                                return DB_FAILED;
                            }
                        }
                        else {
                            //子句判断
                            if (ClauseAnalysis(valMap, typeMap, ast->child_->next_->child_))
                            {
                                //符合条件，可以删
                                if (tableInfo->GetTableHeap()->MarkDelete(rowID, nullptr))
                                {
                                    delNum++;
                                }
                                else {
                                    cout << "minisql: Failed.\n";
                                    return DB_FAILED;
                                }
                            }
                        }
                    }
                    else {
                        cout << "minisql: Failed.\n";
                        return DB_FAILED;
                    }
                }
                
                cout << "minisql: " << "The number of deleted records: " << delNum << ".\n";
                return DB_SUCCESS;
            }
        }
        else {
            cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
}


dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  if (current_db_ != "")
    {
        pSyntaxNode updateNode = ast->child_->next_;
        string tableName = ast->child_->val_;
        uint32_t updateNum = 0;


        //没有索引，用table迭代器遍历
        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
        {
            if (tableInfo->GetTableName() == tableName)
            {
                vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息
                map<string, uint32_t> indexMap; //寻找对应index
                map<string, TypeId> typeMap; //用于寻找对应type的id

                //获取typeMap index与名字映射
                for (uint32_t index = 0; index <= columns.size(); index++)
                {
                    indexMap.insert(pair<string, uint32_t>(columns[index]->GetName(), index));
                    typeMap.insert(pair<string, TypeId>(columns[index]->GetName(), columns[index]->GetType()));
                }


                //初始化Field*数组
                vector<Field*> updateFields(columns.size(), nullptr); //装载的fields

                //处理updatevalues
                for (auto kNode = updateNode->child_; kNode != nullptr; kNode = kNode->next_)
                {
                    string colName = kNode->child_->val_;
                    auto it = typeMap.find(colName);
                    auto indexIt = indexMap.find(colName);
                    if (it != typeMap.end()&& indexIt != indexMap.end())
                    {
                        TypeId type = it->second;

                        Field* inField;
                        string str = kNode->child_->next_->val_;
                        //根据不同类型装载field
                        switch (kNode->child_->next_->type_)
                        {
                        case kNodeNumber:
                            if (type == kTypeInt)
                            {
                                //原数据是整数，输入的数据不是整数报错
                                if (str.find('.') == str.npos)
                                {
                                    int32_t num = atoi(str.c_str());
                                    inField = new Field(type, num);
                                }
                                else {
                                    cout << "minisql[ERROR]: Input error.\n";
                                    return DB_FAILED;
                                }
                            }
                            else {
                                //原数据是小数，输入数据直接装进field，输入整数也当作小数
                                float num = atof(kNode->child_->next_->val_);
                                inField = new Field(type, num);
                            }
                            break;
                        case kNodeString:
                            inField = new Field(type, kNode->child_->next_->val_, uint32_t(str.size() + 1), true);
                            break;
                        default:
                            cout << "MiniSql: Failed.\n";
                            return DB_FAILED;
                            break;
                        }

                        updateFields[indexIt->second] = inField;
                    }
                    else {
                        cout << "minisql: No name.\n";
                        return DB_FAILED;
                    }
                }

                for (auto iter = tableInfo->GetTableHeap()->Begin(nullptr); iter != tableInfo->GetTableHeap()->End(); iter++)
                {
                    //遍历每一行
                    vector<Field*> fields = iter->GetFields();
                    RowId rowID = iter->GetRowId();
                    map<string, Field*> valMap; //用于寻找对应field的map
                    vector<Field> loadField; //最后装载到row的field

                    //判断field和column大小是否相等
                    if (columns.size() == fields.size())
                    {
                        for (uint32_t index = 0; index <= columns.size(); index++)
                        {
                            valMap.insert(pair<string, Field*>(columns[index]->GetName(), fields[index]));
                        }

                        //有没有子句问题
                        if (ast->child_->next_->next_ == nullptr) //没有子句
                        {
                            //没有子句， 直接更新
                            for (size_t i = 0; i < fields.size(); i++)
                            {
                                if (updateFields[i] != nullptr)
                                {
                                    fields[i] = updateFields[i];
                                }

                                loadField.push_back(*fields[i]);
                            }
                        }
                        else {
                            //子句判断
                            if (ClauseAnalysis(valMap, typeMap, ast->child_->next_->next_->child_))
                            {
                                //符合条件，可以更新
                                for (size_t i = 0; i < fields.size(); i++)
                                {
                                    if (updateFields[i] != nullptr)
                                    {
                                        fields[i] = updateFields[i];
                                    }

                                    loadField.push_back(*fields[i]);
                                }
                            }
                        }

                        Row row(loadField);
                        if (tableInfo->GetTableHeap()->UpdateTuple(row, rowID, nullptr))
                        {
                            updateNum++;
                        }
                        else {
                            cout << "minisql: Failed.\n";
                            return DB_FAILED;
                        }
                    }
                    else {
                        cout << "minisql: Failed.\n";
                        return DB_FAILED;
                    }
                }

                cout << "minisql: " << "The number of updated rows: " << updateNum << ".\n";
                return DB_SUCCESS;
            }
        }
        else {
            cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
}






dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}




/// <summary>
/// 执行规定的文件，把文件读取后重新执行指令
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  if (ast->type_ == kNodeExecFile)
    {
        string fileName = ast->child_->val_;

        //读文件
        ifstream fileCMD(fileName);
        const int buf_size = 1024;
        char cmd[buf_size];

        while (1) {
            FileCommand(cmd, buf_size, fileCMD);
            if (fileCMD.eof())
            {
                break;
            }
            
            YY_BUFFER_STATE bp = yy_scan_string(cmd);
            if (bp == nullptr) {
                exit(1);
            }
            yy_switch_to_buffer(bp);

            MinisqlParserInit();

            yyparse();

            if (MinisqlParserGetError()) {
                // error
                printf("%s\n", MinisqlParserGetErrorMessage());
            }

            ExecuteContext fileContext;
            Execute(MinisqlGetParserRootNode(), &fileContext);


            #ifdef _WIN32
            Sleep(1000);
            #else
            sleep(1);
            #endif

            // clean memory after parse
            MinisqlParserFinish();
            yy_delete_buffer(bp);
            yylex_destroy();

            // 在脚本中 quit，直接退到命令行
            if (fileContext.flag_quit_) 
            {
                cout << fileName << ": Bye!" << endl;
                break;
            }
            
        }
        fileCMD.close();

        return DB_SUCCESS;
    }
    else {
        return DB_FAILED;
    }
}

/// <summary>
/// 负责对脚本的读取
/// </summary>
/// <param name="input"></param>
/// <param name="len"></param>
/// <param name="in"></param>
void ExecuteEngine::FileCommand(char* input, const int len, ifstream& in) {
    memset(input, 0, len);
    int i = 0;
    char ch;
    while ((ch = in.get()) != ';') {
        if (!in.eof())
        {
            if (ch == '\n')
            {
                continue;
            }
            input[i++] = ch;
        }
        else {
            return;
        }
    }
    input[i] = ch;    // ;
}


/// <summary>
/// 退出指令，直接退出即可
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
