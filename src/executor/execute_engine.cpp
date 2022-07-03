#include "executor/execute_engine.h"
#include <iostream>
#include <fstream>
#include <cstdio>
#include <string>
#include <iomanip>
#include <map>
#include <cstdlib>
#include <algorithm>
#include <set>


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


using string = std::string;

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
                dbs_.insert(std::pair<string, DBStorageEngine*>(fileinfo.name, dbse));
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



dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext* context) {
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
/// Create数据库 接新建一个对象然后插入map中
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext* context) {
    if (ast->child_->type_ == kNodeIdentifier)
    {
        string dbName = ast->child_->val_;
        dbName += ".db";
        
        if (dbs_.find(dbName) == dbs_.end())
        {
            //Not find
            DBStorageEngine* dbse = new DBStorageEngine(dbPath + "/" + dbName);
            dbs_.insert(std::pair<string, DBStorageEngine*>(dbName, dbse));

            std::cout << "minisql: Create Successfully.\n";
            return DB_SUCCESS;
        }
        else {
            //找到了，报错
            std::cout << "minisql[ERROR]: Database existed.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}

/// <summary>
/// delete指令，删除数据库
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext* context) {
    if (ast->child_->type_ == kNodeIdentifier)
    {
        string dbName = ast->child_->val_; //要删除的数据库名
        string fileName = dbName + ".db";
        std::unordered_map<string, DBStorageEngine*>::iterator it = dbs_.find(fileName);
        if (it != dbs_.end()) //找到
        {
            //先在map中删除
            delete it->second;
            dbs_.erase(it);

            if (remove((dbPath + "/" + fileName).c_str()) == 0)
            {
                //删除成功
                std::cout << "minisql: Delete successfully.\n";

                return DB_SUCCESS;
            }
            else {
                //报错
                std::cout << "minisql[ERROR]: Delete failed.\n" << errno;
                return DB_FAILED;
            }
        }
        else {
            //没找到，报错
            std::cout << "minisql[ERROR]: No database.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}

/// <summary>
///  直接把map里面的列出来就行
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext* context) {
    size_t dbNum = dbs_.size();

    std::cout << "+--------------------+\n";
    std::cout << "| Database           |\n";
    std::cout << "+--------------------+\n";

    if (dbNum != 0)
    {
        for (auto it : dbs_) 
        {
            std::cout << "| " << std::setw(19) << it.first.substr(0, it.first.size() - 3) << "|" << std::endl;
        }
        std::cout << "+--------------------+\n";
    }

    std::cout << "The number of databases: " << dbNum << std::endl;
    return DB_SUCCESS;
}

/// <summary>
/// 切换当前数据库
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext* context) {
    if (ast->child_->type_ == kNodeIdentifier)
    {
        string dbName = ast->child_->val_; //要切换的数据库名称
        string fileName = dbName + ".db";

        std::unordered_map<string, DBStorageEngine*>::iterator it = dbs_.find(fileName);
        if (it != dbs_.end())
        {
            //找到
            current_db_ = it->first;
            curDB = it->second;
            std::cout << "minisql: Database changed.\n";
            return DB_SUCCESS;
        }
        else {
            //没找到
            std::cout << "minisql[ERROR]: No database.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}










/// <summary>
/// 列出数据库下的所有表
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext* context) {
    if (current_db_ != "")
    {
        std::vector<TableInfo*> tables;
        if (curDB->catalog_mgr_->GetTables(tables) == DB_SUCCESS)
        {
            int i = 0;
            for (auto it : tables)
            {
                if (i != 0)
                {
                    std::cout << std::endl << it->GetTableName();
                }
                else {
                    std::cout << it->GetTableName();
                    i++;
                }
            }
            std::cout << std::endl << "The number of tables: " << tables.size() << std::endl;
            return DB_SUCCESS;
        }
        else {
            std::cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}



/// <summary>
/// 创建table
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext* context) {
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
                std::cout << "minisql: Table existed.\n";
                return DB_FAILED;
            }
        }



        pSyntaxNode defList = ast->child_->next_;
        if (defList->child_ != nullptr)
        {
            std::vector<Column*> columns;

            pSyntaxNode colDef;
            uint32_t curIndex = 0;
            std::map<string, uint32_t> indexMap; 
            std::vector<string> uniqueNames;
            for (colDef = defList->child_; colDef->type_ != kNodeColumnList && colDef != nullptr; colDef = colDef->next_, curIndex++)
            {
                string name = colDef->child_->val_;
                TypeId type = kTypeInvalid;
                uint32_t index = curIndex;
                bool nullable = true;
                bool unique = false;
                indexMap.insert(std::pair<string, uint32_t>(name, index));

                if (colDef->val_ != nullptr)
                {
                    string colVal = colDef->val_;
                    if (colVal == "unique")
                    {
                        unique = true;
                        uniqueNames.push_back(name);
                    }
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
                        std::cout << "minisql: Input error.\n";
                        return DB_FAILED;
                    }
                }
             
            }

            //主键处理
            std::vector<string> pkNames;
            if (colDef != nullptr)
            {
                if (colDef->type_ == kNodeColumnList)
                {
                    int* nullArray = new int[curIndex]();

                    for (pSyntaxNode pkNode = colDef->child_; pkNode != nullptr; pkNode = pkNode->next_)
                    {
                        string pkName = pkNode->val_; //主键名字

                        auto it = indexMap.find(pkName);
                        if (it != indexMap.end())
                        {
                            nullArray[it->second] = 1;
                            pkNames.push_back(pkName);
                        }
                        else {
                            std::cout << "minisql: Input error.\n";
                            return DB_FAILED;
                        }
                    }

                    for (uint32_t i = 0; i < curIndex; i++)
                    {
                        if (nullArray[i] != 1)
                        {
                            //不是主键
                            columns[i]->SetNullable(true);
                        }
                        else {
                            //是主键
                            columns[i]->SetNullable(false);
                            if (pkNames.size() == 1)
                            {
                                columns[i]->SetUnique(true);
                            }
                        }
                    }
                }
            }

            //创建table，组装
            TableSchema* schema = new TableSchema(columns);
            if (curDB->catalog_mgr_->CreateTable(tableName, schema, nullptr, tableInfo) == DB_SUCCESS)
            {
                IndexInfo* pkinfo = IndexInfo::Create(new SimpleMemHeap());
                string pkindexName = ";PK" + tableName;
                curDB->catalog_mgr_->CreateIndex(tableName, pkindexName, pkNames, nullptr, pkinfo);
                
                for (auto uniCol : uniqueNames)
                {
                    std::vector<string> uname;
                    uname.push_back(uniCol);
                    IndexInfo* uinfo = IndexInfo::Create(new SimpleMemHeap());
                    string uindexName = ";UNI" + tableName + uniCol;
                    curDB->catalog_mgr_->CreateIndex(tableName, uindexName, uname, nullptr, uinfo);
                }
                std::cout << "minisql: Create table successfully.\n";
                return DB_SUCCESS;
            }
            else {
                std::cout << "minisql: Failed.\n";
                return DB_FAILED;
            }
        }
        else {
            std::cout << "minisql: Input error.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}



/// <summary>
/// 删除table
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext* context) {
    if (ast->child_->type_ == kNodeIdentifier)
    {
        if (current_db_ != "")
        {
            string tableName = ast->child_->val_;
            if (curDB->catalog_mgr_->DropTable(tableName) == DB_SUCCESS)
            {
                std::cout << "minisql: Delete successfully.\n";
                return DB_SUCCESS;
            }
            else {
                std::cout << "minisql[ERROR]: No table.\n";
                return DB_FAILED;
            }
        }
        else {
            std::cout << "minisql: No database selected.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}





/// <summary>
/// 显示某个数据库下的所有index
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext* context) {
    if (current_db_ != "")
    {
        //先获得所有table
        std::vector<TableInfo*> tables;
        if (curDB->catalog_mgr_->GetTables(tables) == DB_SUCCESS)
        {   
            int i = 0;
            for (auto tb : tables)
            {
                std::vector<IndexInfo*> indexes;
                if (curDB->catalog_mgr_->GetTableIndexes(tb->GetTableName(), indexes) == DB_SUCCESS)
                {
                    size_t sum = 0;
                    for (auto it : indexes)
                    {
                        string indexName = it->GetIndexName();
                        if (indexName[0] == ';')
                        {
                            continue;
                        }
                        sum++;
                        if (i != 0)
                        {
                            std::cout << std::endl << tb->GetTableName() << ": " << it->GetIndexName();
                        }
                        else {
                            std::cout << tb->GetTableName() << ": " << it->GetIndexName();
                            i++;
                        }
                    }
                    std::cout << std::endl << "The number of indexes: " << sum << std::endl;
                    return DB_SUCCESS;
                }
                else {
                    std::cout << "minisql[ERROR]: Failed.\n";
                    return DB_FAILED;
                }
            }
        }
        else {
            std::cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}



/// <summary>
/// 创建index
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext* context) {
    if (current_db_ != "")
    {
        string indexName = ast->child_->val_;
        string tableName = ast->child_->next_->val_;


        std::vector<Column*> columns; //把列记录，之后用

        //不能跟已有的重名
        std::vector<TableInfo*> tables;
        if (curDB->catalog_mgr_->GetTables(tables) == DB_SUCCESS)
        {
            for (auto tb : tables)
            {
                std::vector<IndexInfo*> indexes;
                if (tb->GetTableName() == tableName)
                {
                    columns = tb->GetSchema()->GetColumns();
                }

                if (curDB->catalog_mgr_->GetTableIndexes(tb->GetTableName(), indexes) == DB_SUCCESS)
                {
                    for (auto it : indexes)
                    {
                        if (it->GetIndexName() == indexName)
                        {
                            std::cout << "minisql: Index existed.\n";
                            return DB_FAILED;
                        }
                    }
                }
                else {
                    std::cout << "minisql[ERROR]: Failed.\n";
                    return DB_FAILED;
                }
            }

            if (columns.size() == 0)
            {
                std::cout << "minisql[ERROR]: No table.\n";
                return DB_FAILED;
            }
        }
        else {
            std::cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }

        std::map<string, Column*> colMap;
        for (auto p : columns)
        {
            colMap.insert(std::pair<string, Column*>(p->GetName(), p));
        }


        //把column写进vector
        pSyntaxNode colList = ast->child_->next_->next_;
        if (colList != nullptr)
        {
            std::vector<string> index_keys;

            for (auto col = colList->child_; col != nullptr; col = col->next_)
            {
                string cName = col->val_;

                //看看有没对应的列
                auto it = colMap.find(cName);
                if (it == colMap.end())
                {
                    //没查到
                    std::cout << "minisql: No column.\n";
                    return DB_FAILED;
                }
                index_keys.push_back(cName);
            }

            for (string indexCol : index_keys)
            {
                auto i = colMap.find(indexCol);
                if (i != colMap.end())
                {
                    //查到
                    if (!i->second->IsUnique())
                    {
                        std::cout << "minisql: Exist non-unique column.\n";
                        return DB_FAILED;
                    }
                }
            }

            IndexInfo* index_info = IndexInfo::Create(new SimpleMemHeap());
            if (colList->next_ == nullptr)
            {
                if (curDB->catalog_mgr_->CreateIndex(tableName, indexName, index_keys, nullptr, index_info) == DB_SUCCESS)
                {
                    TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
                    if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
                    {
                        //把现在数据InsertEntry
                        for (auto iter = tableInfo->GetTableHeap()->Begin(nullptr); iter != tableInfo->GetTableHeap()->End(); iter++)
                        {
                            //遍历每一行
                            std::vector<Field*> fields = iter->GetFields();
                            std::map<string, Field*> valMap; //用于寻找对应field的map

                            //判断field和column大小是否相等
                            if (columns.size() == fields.size())
                            {
                                for (uint32_t index = 0; index < columns.size(); index++)
                                {
                                    valMap.insert(std::pair<string, Field*>(columns[index]->GetName(), fields[index]));
                                }

                                std::vector<Field> indexfields;
                                for (string indexCol : index_keys)
                                {
                                    auto i = valMap.find(indexCol);
                                    if (i != valMap.end())
                                    {
                                        //查到了
                                        indexfields.push_back(*(i->second));
                                    }
                                }

                                Row indexRow(indexfields);
                                uint32_t size = indexRow.GetSerializedSize(nullptr);
                                if (size > 32)
                                {
                                    std::cout << "minisql: Too long key.\n";
                                    curDB->catalog_mgr_->DropIndex(tableName, indexName);
                                    return DB_FAILED;
                                }
                                RowId rid = iter->GetRowId();
                                if (index_info->GetIndex()->InsertEntry(indexRow, rid, nullptr) == DB_SUCCESS)
                                {
                                }
                                else {
                                    std::cout << "minisql: Failed.\n";
                                    return DB_FAILED;
                                }
                            }
                            else {
                                std::cout << "minisql: Failed.\n";
                                return DB_FAILED;
                            }
                        }
                        std::cout << "minisql: Create index successfully.\n";
                        return DB_SUCCESS;
                    }
                    else {
                        std::cout << "minisql[ERROR]: Failed.\n";
                        return DB_FAILED;
                    }
                }
                else {
                    std::cout << "minisql[ERROR]: Failed.\n";
                    return DB_FAILED;
                }
            }
            else {
                //特定的数据结构
                pSyntaxNode indexType = colList->next_->child_;
                if (indexType != nullptr)
                {
                    string indextypeName = indexType->val_;

                    if (indextypeName != "bptree")
                    {
                        std::cout << "minisql[ERROR]: No support.\n";
                        return DB_FAILED;
                    }
                    else {
                        std::cout << "minisql[ERROR]: Please use create index without using.\n";
                    }
                    return DB_SUCCESS;
                }
                else {
                    std::cout << "minisql[ERROR]: Failed.\n";
                    return DB_FAILED;
                }
            }
        }
        else {
            std::cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}







/// <summary>
/// 删除索引，不能指定特定的表（受解释器限制）
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext* context) {
    if (ast->child_->type_ == kNodeIdentifier)
    {
        string indexName = ast->child_->val_;
        //先找出来索引对应的table
        std::vector<TableInfo*> tables;
        if (curDB->catalog_mgr_->GetTables(tables) == DB_SUCCESS)
        {
            for (auto it : tables)
            {
                  //语句不支持输入table的名字，只能一个一个试
                if (curDB->catalog_mgr_->DropIndex(it->GetTableName(), indexName) == DB_SUCCESS)
                {
                    std::cout << "minisql: Delete index " << indexName << "." << std::endl;
                    return DB_SUCCESS;
                }
            }
            std::cout << "minisql: No index.\n";
            return DB_FAILED;
        }
        else {
            std::cout << "minisql[ERROR]: Failed.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql[ERROR]: Input error.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}





/// <summary>
/// where子句分析
/// </summary>
/// <param name="valMap"></param>
/// <param name="typeMap"></param>
/// <param name="kNode"></param>
/// <returns></returns>
bool ExecuteEngine::ClauseAnalysis(std::map<string, Field*>& valMap, std::map<string, TypeId>& typeMap, pSyntaxNode kNode)
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
                    std::cout << "MiniSql: Input error\n";
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
                    std::cout << "MiniSql: Input error\n";
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
                        std::cout << "MiniSql: Use is/not instead of \'=\' or \'<>\'.\n";
                        return false;
                    }

                    Field* inField;
                    //根据不同类型装载field
                    
                    switch (kNode->child_->next_->type_)
                    {
                    case kNodeNumber:
                        if (type == kTypeInt)
                        {
                            //  原数据是整数，输入的数据不是整数报错
                            if (str.find('.') == str.npos)
                            {
                                int32_t num = atoi(str.c_str());
                                inField = new Field(type, num);
                            }
                            else {
                                std::cout << "minisql[ERROR]: Input error.\n";
                                return DB_FAILED;
                            }
                        }
                        else {
                            // 原数据是小数，输入数据直接装进field，输入整数也当作小数
                            float num = atof(kNode->child_->next_->val_);
                            inField = new Field(type, num);
                        }
                        break;
                    case kNodeString:
                        inField = new Field(type, kNode->child_->next_->val_, dataField->GetLength(), true);
                        break;
                    default:
                        std::cout << "MiniSql: Failed.\n";
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
                            if (dataField->GetType() == kTypeChar && inField->GetType() == kTypeChar)
                            {
                                string a = dataField->GetChars();
                                string b = inField->GetChars();
                                if (a == b)
                                {
                                    return true;
                                }
                                else {
                                    return false;
                                }
                            }
                            res = dataField->CompareEquals(*inField);
                            if (res != kNull)
                            {
                                return res == kTrue ? true : false;
                            }
                            else {
                                std::cout << "MiniSql: Failed\n";
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
                                    std::cout << "MiniSql: Failed\n";
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
                                    std::cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else if (val == "<>")
                            {
                                if (dataField->GetType() == kTypeChar && inField->GetType() == kTypeChar)
                                {
                                    string a = dataField->GetChars();
                                    string b = inField->GetChars();
                                    if (a == b)
                                    {
                                        return false;
                                    }
                                    else {
                                        return true;
                                    }
                                }
                                res = dataField->CompareNotEquals(*inField);
                                if (res != kNull)
                                {
                                    return res == kTrue ? true : false;
                                }
                                else {
                                    std::cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else {
                                std::cout << "MiniSql: Failed\n";
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
                                    std::cout << "MiniSql: Failed\n";
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
                                    std::cout << "MiniSql: Failed\n";
                                    return false;
                                }
                            }
                            else {
                                std::cout << "MiniSql: Failed\n";
                                return false;
                            }
                            break;
                        default:
                            std::cout << "MiniSql: Failed\n";
                            return false;
                            break;
                        }

                    }
                    else {
                        std::cout << "MiniSql: Type error\n";
                        return false;
                    }
                }
                else {
                    std::cout << "MiniSql: Input error\n";
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
                std::cout << "Clause error\n";
                return false;
            }
            break;
        default:
            std::cout << "Clause error\n";
            return false;
            break;
        }
    }
    else {
        std::cout << "Clause error\n";
        return false;
    }
    return false;
}

/// <summary>
/// 子句提取
/// </summary>
/// <param name="kNode"></param>
/// <param name="colNameSet"></param>
/// <param name="parserIndexRes"></param>
/// <param name="parserEtcRes"></param>
/// <returns></returns>
bool ExecuteEngine::ClauseAndParser(std::map<std::string, TypeId>& typeMap, std::map<std::string, uint32_t>& lengthMap, pSyntaxNode kNode, std::set<string>& colNameSet, std::map<std::string, fieldCmp>& parserIndexRes, std::map<std::string, fieldCmp>& parserEtcRes)
{
    if (kNode->child_ != nullptr && kNode->child_->next_ != nullptr)
    {
        string val = kNode->val_;
        string name = "";
        std::map<std::string, TypeId>::iterator typeIter;
        std::map<std::string, uint32_t>::iterator lengthIter;
        bool isIndex = false;
        switch (kNode->type_)
        {
        case kNodeCompareOperator:
            name = kNode->child_->val_;
            if (colNameSet.find(name) != colNameSet.end())//找到
            {
                isIndex = true;
            }

            typeIter = typeMap.find(name);
            lengthIter = lengthMap.find(name);
            if (typeIter == typeMap.end() || lengthIter == lengthMap.end())
            {
                std::cout << "Clause error\n";
                return false;
            }

            if (val == "is")
            {
                fieldCmp fcmp{ new Field(typeIter->second), "=" };
                if (isIndex)
                {
                    parserIndexRes.insert(std::pair<string, fieldCmp>(name, fcmp));
                }
                else {
                    parserEtcRes.insert(std::pair<string, fieldCmp>(name, fcmp));
                }
                return true;
            }
            else if (val == "not")
            {
                fieldCmp fcmp{ new Field(typeIter->second), "<>" };
                if (isIndex)
                {
                    parserIndexRes.insert(std::pair<string, fieldCmp>(name, fcmp));
                }
                else {
                    parserEtcRes.insert(std::pair<string, fieldCmp>(name, fcmp));
                }
                return true;
            }
            else {
                string str = kNode->child_->next_->val_; //语法树的原始比较数

                if (kNode->child_->next_->type_ == kNodeNull)
                {
                    std::cout << "MiniSql: Use is/not instead of \'=\' or \'<>\'.\n";
                    return false;
                }

                Field* f;

                switch (kNode->child_->next_->type_)
                {
                case kNodeNumber:
                    if (typeIter->second == kTypeInt)
                    {
                        //原数整数，输入的数据不是整数报错
                        if (str.find('.') == str.npos)
                        {
                            int32_t num = atoi(str.c_str());
                            f = new Field(typeIter->second, num);
                        }
                        else {
                            std::cout << "minisql[ERROR]: Input error.\n";
                            return DB_FAILED;
                        }
                    }
                    else {
                        //原数小数，输入数据直接装进field，输入整数也当作小数
                        float num = atof(kNode->child_->next_->val_);
                        f = new Field(typeIter->second, num);
                    }
                    break;
                case kNodeString:
                    f = new Field(typeIter->second, kNode->child_->next_->val_, lengthIter->second, true);
                    break;
                default:
                    std::cout << "MiniSql: Failed.\n";
                    return false;
                    break;
                }

                fieldCmp fcmp{ f, val };
                if (isIndex)
                {
                    parserIndexRes.insert(std::pair<string, fieldCmp>(name, fcmp));
                }
                else {
                    parserEtcRes.insert(std::pair<string, fieldCmp>(name, fcmp));
                }
                return true;
            }
            break;
        case kNodeConnector:
            if (val == "and")
            {
                return ClauseAndParser(typeMap, lengthMap, kNode->child_, colNameSet, parserIndexRes, parserEtcRes) && ClauseAndParser(typeMap, lengthMap, kNode->child_->next_, colNameSet, parserIndexRes, parserEtcRes);
            }
            else if (val == "or") //index不处理or请求，只支持所有的and
            {
                return false;
            }
            else {
                std::cout << "Clause error\n";
                return false;
            }
            break;
        default:
            std::cout << "Clause error\n";
            return false;
            break;
        }
    }
    else {
        std::cout << "Clause error\n";
        return false;
    }
    return false;
}



bool ExecuteEngine::RecordJudge(Row& row, std::map<std::string, fieldCmp>& parser, std::map<std::string, uint32_t>& idxMap)
{
    if (parser.size() == 0)
    {
        return true;
    }

    for (auto& iter : parser)
    {
        string val = iter.second.cmp;
        auto i = idxMap.find(iter.first);

        if (i != idxMap.end()) //找到了
        {
            Field* dataField = row.GetField(i->second);
            Field* inField = iter.second.field;
            CmpBool res;
            if (dataField->CheckComparable(*inField))
            {
                switch (val[0])
                {
                case '=':
                    if (dataField->IsNull() || inField->IsNull())
                    {
                        if (dataField->IsNull() != inField->IsNull())
                        {
                            return false;
                        }
                    }
                    else {
                        if (dataField->GetType() == kTypeChar && inField->GetType() == kTypeChar)
                        {
                            string a = dataField->GetChars();
                            string b = inField->GetChars();
                            if (a == b)
                            {
                                return true;
                            }
                            else {
                                return false;
                            }
                        }
                        res = dataField->CompareEquals(*inField);
                        if (res != kNull)
                        {
                            if (res == kFalse)
                            {
                                return false;
                            }
                        }
                        else {
                            std::cout << "MiniSql: Failed\n";
                            return false;
                        }
                    }
                    break;
                case '<':
                    if (val == "<=")
                    {
                        res = dataField->CompareLessThanEquals(*inField);
                        if (res != kNull)
                        {
                            if (res == kFalse)
                            {
                                return false;
                            }
                        }
                        else {
                            std::cout << "MiniSql: Failed\n";
                            return false;
                        }
                    }
                    else if (val == "<")
                    {
                        res = dataField->CompareLessThan(*inField);
                        if (res != kNull)
                        {
                            if (res == kFalse)
                            {
                                return false;
                            }
                        }
                        else {
                            std::cout << "MiniSql: Failed\n";
                            return false;
                        }
                    }
                    else if (val == "<>")
                    {
                        if (dataField->IsNull() || inField->IsNull())
                        {
                            if (dataField->IsNull() == inField->IsNull())
                            {
                                return false;
                            }
                        }
                        else {
                            if (dataField->GetType() == kTypeChar && inField->GetType() == kTypeChar)
                            {
                                string a = dataField->GetChars();
                                string b = inField->GetChars();
                                if (a == b)
                                {
                                    return false;
                                }
                                else {
                                    return true;
                                }
                            }
                            res = dataField->CompareNotEquals(*inField);
                            if (res != kNull)
                            {
                                if (res == kFalse)
                                {
                                    return false;
                                }
                            }
                            else {
                                std::cout << "MiniSql: Failed\n";
                                return false;
                            }
                        }
                    }
                    else {
                        std::cout << "MiniSql: Failed\n";
                        return false;
                    }
                    break;
                case '>':
                    if (val == ">=")
                    {
                        res = dataField->CompareGreaterThanEquals(*inField);
                        if (res != kNull)
                        {
                            if (res == kFalse)
                            {
                                return false;
                            }
                        }
                        else {
                            std::cout << "MiniSql: Failed\n";
                            return false;
                        }
                    }
                    else if (val == ">")
                    {
                        res = dataField->CompareGreaterThan(*inField);
                        if (res != kNull)
                        {
                            if (res == kFalse)
                            {
                                return false;
                            }
                        }
                        else {
                            std::cout << "MiniSql: Failed\n";
                            return false;
                        }
                    }
                    else {
                        std::cout << "MiniSql: Failed\n";
                        return false;
                    }
                    break;
                default:
                    std::cout << "MiniSql: Failed\n";
                    return false;
                    break;
                }

            }
            else {
                std::cout << "MiniSql: Type error\n";
                return false;
            }
        }
        else {
            std::cout << "MiniSql: Failed\n";
            return false;
        }
    }

    return true;
}


/// <summary>
/// 选择
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext* context) {
    if (current_db_ != "")
    {
        pSyntaxNode printNode = ast->child_;
        string tableName = ast->child_->next_->val_;
        uint32_t selNum = 0;

        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
        {
            if (tableInfo->GetTableName() == tableName)
            {
                std::vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息
                std::map<string, TypeId> typeMap; //用于寻找对应type的id
                std::map<string, uint32_t> lengthMap;
                std::map<string, uint32_t> idxMap; //寻找对应列号
                int* isPrint = new int[columns.size()](); //默认均不输出
                
                if (printNode->type_ != kNodeAllColumns)
                {
                      //看要输出哪几列
                    std::map<string, uint32_t> nameMap; //名字到index的map

                    for (auto col : columns)
                    {
                        nameMap.insert(std::pair<string, uint32_t>(col->GetName(), col->GetTableInd()));
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
                            std::cout << "minisql: Input error.\n";
                            return DB_FAILED;
                        }
                    }
                }
                else {
                    for (size_t i = 0; i < columns.size(); i++)
                    {
                        //全设为1
                        isPrint[i] = 1;
                    }
                }
                
                for (uint32_t index = 0; index < columns.size(); index++)
                {
                    typeMap.insert(std::pair<string, TypeId>(columns[index]->GetName(), columns[index]->GetType()));
                    lengthMap.insert(std::pair<string, uint32_t>(columns[index]->GetName(), columns[index]->GetLength()));
                    idxMap.insert(std::pair<string, uint32_t>(columns[index]->GetName(), index));
                    //先输出列名
                    if (isPrint[index])
                    {
                        std::cout << std::setw(8) << columns[index]->GetName() << "\t";
                    }
                }
                std::cout << std::endl;

                if (ast->child_->next_->next_ != nullptr) 
                {
                    std::vector<IndexInfo*> indexes;
                    if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                    {
                        if (indexes.size() != 0)
                        {
                            IndexInfo* indexinfoFinal = nullptr;
                            std::map<std::string, fieldCmp> indexFinal; //子句
                            std::map<std::string, fieldCmp> etcFinal; //子句
                            size_t etcSize = SIZE_MAX; //选etcsize最小的

                            //遍历所有index
                            for (auto indexinfo : indexes)
                            {
                                string indexName = indexinfo->GetIndexName();
                                if (indexName[0] == ';')
                                {
                                    continue;
                                }
                                //每个index
                                std::map<std::string, fieldCmp> indexParser;
                                std::map<std::string, fieldCmp> etcParser;
                                std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                std::set<string> colNameSet; //index列名集合
                                for (auto col : cols)
                                {
                                    string colName = col->GetName();
                                    colNameSet.insert(colName);
                                }
                                if (ClauseAndParser(typeMap, lengthMap, ast->child_->next_->next_->child_, colNameSet, indexParser, etcParser))//解析子句成分
                                {
                                    if (colNameSet.size() == indexParser.size())
                                    {
                                        //符合index查询要求
                                        if (etcParser.size() < etcSize)
                                        {
                                            etcSize = etcParser.size();
                                            indexFinal = indexParser;
                                            etcFinal = etcParser;
                                            indexinfoFinal = indexinfo;
                                        }
                                    }
                                }
                            }

                            if (indexinfoFinal != nullptr)
                            {

                                std::vector<Field> keyFields;
                                std::vector<Column*> keyCols = indexinfoFinal->GetIndexKeySchema()->GetColumns(); //正确顺序
                                for (auto keyCol : keyCols)
                                {
                                    auto i = indexFinal.find(keyCol->GetName());

                                    if (i != indexFinal.end())
                                    {
                                        keyFields.push_back(*(i->second.field));
                                    }
                                    else {
                                        std::cout << "minisql: Failed.\n";
                                        return DB_FAILED;
                                    }
                                }

                                Row keyRow(keyFields); //索引key

                                GenericKey<32> gKey;
                                gKey.SerializeFromKey(keyRow, indexinfoFinal->GetIndexKeySchema());

                                BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>* bpindex = dynamic_cast<BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>*>(indexinfoFinal->GetIndex());
                                if (!bpindex->IsEmpty())
                                {
                                    std::vector<RowId> ids;
                                    if (bpindex->ScanKey(keyRow, ids, nullptr) != DB_KEY_NOT_FOUND)
                                    {
                                        IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> goalIter = bpindex->GetBeginIterator(gKey);
                                        Row goalRow((*goalIter).second);
                                        if (tableInfo->GetTableHeap()->GetTuple(&goalRow, nullptr))
                                        {

                                        }
                                        else {
                                            std::cout << "minisql: Row Failed.\n";
                                            return DB_FAILED;
                                        }
                                        std::vector<Field*> goalFields = goalRow.GetFields();
                                        IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> endIter = goalIter;
                                        //模式选择
                                        int cmpState = -2; //-1 直接用tableiter 0完全等于 1大于iter范围 2小于iter范围
                                        for (auto& iter : indexFinal)
                                        {
                                            string cmp = iter.second.cmp;

                                            if (cmp == "<>")
                                            {
                                                cmpState = -1;
                                                break;
                                            }
                                            else if (cmp == "=")
                                            {
                                                if (cmpState == -2)
                                                {
                                                    cmpState = 0;
                                                }
                                            }
                                            else if (cmp == "<" || cmp == "<=")
                                            {
                                                if (cmpState == -2 || cmpState == 0)
                                                {
                                                    cmpState = 2;
                                                }
                                                else if (cmpState == 1)
                                                {
                                                    cmpState = -1;
                                                    break;
                                                }

                                            }
                                            else if (cmp == ">" || cmp == ">=")
                                            {
                                                if (cmpState == -2 || cmpState == 0)
                                                {
                                                    cmpState = 1;
                                                }
                                                else if (cmpState == 2)
                                                {
                                                    cmpState = -1;
                                                    break;
                                                }
                                            }
                                        }
                                        switch (cmpState)
                                        {
                                        case 0:
                                            if (RecordJudge(goalRow, etcFinal, idxMap))
                                            {
                                                selNum++;
                                                bool flag = false;
                                                for (uint32_t index = 0; index < columns.size(); index++)
                                                {
                                                    if (isPrint[index])
                                                    {
                                                        Field* field = goalFields[index];
                                                        if (field->IsNull())
                                                        {
                                                            if (flag == false)
                                                            {
                                                                std::cout << std::setw(8) << "NULL";
                                                                flag = true;
                                                            }
                                                            else {
                                                                std::cout << "\t" << std::setw(8) << "NULL";
                                                            }
                                                        }
                                                        else {
                                                            string data;
                                                            char* s = new char[40];
                                                            switch (field->GetType())
                                                            {
                                                            case kTypeInt:
                                                                data = std::to_string(field->GetInteger());
                                                                break;
                                                            case kTypeFloat:
                                                                sprintf(s, "%.2f", field->GetFloat());
                                                                data = s;
                                                                delete[] s;
                                                                break;
                                                            case kTypeChar:
                                                                data = field->GetChars();
                                                                break;
                                                            default:
                                                                break;
                                                            }

                                                            if (flag == false)
                                                            {
                                                                std::cout << std::setw(8) << data;
                                                                flag = true;
                                                            }
                                                            else {
                                                                std::cout << "\t" << std::setw(8) << data;
                                                            }
                                                        }
                                                    }
                                                }
                                                std::cout << std::endl;
                                            }
                                            if (selNum == 0)
                                            {
                                                std::cout << "Empty set.\n";
                                            }
                                            else {
                                                std::cout << "minisql: " << "The number of selected rows: " << selNum << ".\n";
                                            }
                                            return DB_SUCCESS;
                                            break;
                                        case 1:
                                            //大于范围
                                            for (auto idxIter = goalIter; idxIter != bpindex->GetEndIterator(); ++idxIter)
                                            {
                                                Row thisRow((*idxIter).second);
                                                if (tableInfo->GetTableHeap()->GetTuple(&thisRow, nullptr))
                                                {

                                                }
                                                else {
                                                    std::cout << "minisql: Row Failed.\n";
                                                    return DB_FAILED;
                                                }
                                                std::vector<Field*> fields = thisRow.GetFields();
                                                if (RecordJudge(thisRow, etcFinal, idxMap) && RecordJudge(thisRow, indexFinal, idxMap))
                                                {
                                                    selNum++;
                                                    bool flag = false;
                                                    for (uint32_t index = 0; index < columns.size(); index++)
                                                    {
                                                        if (isPrint[index])
                                                        {
                                                            Field* field = fields[index];
                                                            if (field->IsNull())
                                                            {
                                                                if (flag == false)
                                                                {
                                                                    std::cout << std::setw(8) << "NULL";
                                                                    flag = true;
                                                                }
                                                                else {
                                                                    std::cout << "\t" << std::setw(8) << "NULL";
                                                                }
                                                            }
                                                            else {
                                                                string data;
                                                                char* s = new char[40];
                                                                switch (field->GetType())
                                                                {
                                                                case kTypeInt:
                                                                    data = std::to_string(field->GetInteger());
                                                                    break;
                                                                case kTypeFloat:
                                                                    sprintf(s, "%.2f", field->GetFloat());
                                                                    data = s;
                                                                    delete[] s;
                                                                    break;
                                                                case kTypeChar:
                                                                    data = field->GetChars();
                                                                    break;
                                                                default:
                                                                    break;
                                                                }

                                                                if (flag == false)
                                                                {
                                                                    std::cout << std::setw(8) << data;
                                                                    flag = true;
                                                                }
                                                                else {
                                                                    std::cout << "\t" << std::setw(8) << data;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    std::cout << std::endl;
                                                }
                                            }
                                            if (selNum == 0)
                                            {
                                                std::cout << "Empty set.\n";
                                            }
                                            else {
                                                std::cout << "minisql: " << "The number of selected rows: " << selNum << ".\n";
                                            }
                                            return DB_SUCCESS;
                                            break;
                                        case 2:
                                            //小于范围
                                            ++endIter;
                                            for (auto idxIter = bpindex->GetBeginIterator(); idxIter != endIter; ++idxIter)
                                            {
                                                Row thisRow((*idxIter).second);
                                                if (tableInfo->GetTableHeap()->GetTuple(&thisRow, nullptr))
                                                {

                                                }
                                                else {
                                                    std::cout << "minisql: Row Failed.\n";
                                                    return DB_FAILED;
                                                }
                                                std::vector<Field*> fields = thisRow.GetFields();
                                                if (RecordJudge(thisRow, etcFinal, idxMap) && RecordJudge(thisRow, indexFinal, idxMap))
                                                {
                                                    selNum++;
                                                    bool flag = false;
                                                    for (uint32_t index = 0; index < columns.size(); index++)
                                                    {
                                                        if (isPrint[index])
                                                        {
                                                            Field* field = fields[index];
                                                            if (field->IsNull())
                                                            {
                                                                if (flag == false)
                                                                {
                                                                    std::cout << std::setw(8) << "NULL";
                                                                    flag = true;
                                                                }
                                                                else {
                                                                    std::cout << "\t" << std::setw(8) << "NULL";
                                                                }
                                                            }
                                                            else {
                                                                string data;
                                                                char* s = new char[40];
                                                                switch (field->GetType())
                                                                {
                                                                case kTypeInt:
                                                                    data = std::to_string(field->GetInteger());
                                                                    break;
                                                                case kTypeFloat:
                                                                    sprintf(s, "%.2f", field->GetFloat());
                                                                    data = s;
                                                                    delete[] s;
                                                                    break;
                                                                case kTypeChar:
                                                                    data = field->GetChars();
                                                                    break;
                                                                default:
                                                                    break;
                                                                }

                                                                if (flag == false)
                                                                {
                                                                    std::cout << std::setw(8) << data;
                                                                    flag = true;
                                                                }
                                                                else {
                                                                    std::cout << "\t" << std::setw(8) << data;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    std::cout << std::endl;
                                                }
                                            }
                                            if (selNum == 0)
                                            {
                                                std::cout << "Empty set.\n";
                                            }
                                            else {
                                                std::cout << "minisql: " << "The number of selected rows: " << selNum << ".\n";
                                            }
                                            return DB_SUCCESS;
                                            break;
                                        default:
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }


                //没有索引，用table迭代器遍历
                for (auto iter = tableInfo->GetTableHeap()->Begin(nullptr); iter != tableInfo->GetTableHeap()->End(); iter++)
                {
                    //遍历每一行
                    std::vector<Field*> fields = iter->GetFields();
                    std::map<string, Field*> valMap; //用于寻找对应field的map

                    //判断field和column大小是否相等
                    if (columns.size() == fields.size())
                    {
                        for (uint32_t index = 0; index < columns.size(); index++)
                        {
                            valMap.insert(std::pair<string, Field*>(columns[index]->GetName(), fields[index]));
                        }

                        //有没有子句问题
                        if (ast->child_->next_->next_ == nullptr) //没有子句
                        {
                            //没有子句，直接输出
                            selNum++;
                            bool flag = false;
                            for (uint32_t index = 0; index < columns.size(); index++)
                            {
                                if (isPrint[index])
                                {
                                    Field* field = fields[index];
                                    if (field->IsNull())
                                    {
                                        if (flag == false)
                                        {
                                            std::cout << std::setw(8) << "NULL";
                                            flag = true;
                                        }
                                        else {
                                            std::cout << "\t" << std::setw(8) << "NULL";
                                        }
                                    }
                                    else {
                                        string data;
                                        char* s = new char[40];
                                        switch (field->GetType())
                                        {
                                        case kTypeInt:
                                            data = std::to_string(field->GetInteger());
                                            break;
                                        case kTypeFloat:
                                            sprintf(s, "%.2f", field->GetFloat());
                                            data = s;
                                            delete[] s;
                                            break;
                                        case kTypeChar:
                                            data = field->GetChars();
                                            break;
                                        default:
                                            break;
                                        }

                                        if (flag == false)
                                        {
                                            std::cout << std::setw(8) << data;
                                            flag = true;
                                        }
                                        else {
                                            std::cout << "\t" << std::setw(8) << data;
                                        }
                                    }
                                }
                            }
                            std::cout << std::endl;
                        }
                        else {
                            //子句判断
                            if (ClauseAnalysis(valMap, typeMap, ast->child_->next_->next_->child_))
                            {
                                selNum++;
                                bool flag = false;
                                //符合条件，可以输出
                                for (uint32_t index = 0; index < columns.size(); index++)
                                {
                                    if (isPrint[index])
                                    {
                                        Field* field = fields[index];
                                        if (field->IsNull())
                                        {
                                            if (flag == false)
                                            {
                                                std::cout << "NULL";
                                                flag = true;
                                            }
                                            else {
                                                std::cout << "\t" << "NULL";
                                            }
                                        }
                                        else {
                                            string data;
                                            char* s = new char[40];
                                            switch (field->GetType())
                                            {
                                            case kTypeInt:
                                                data = std::to_string(field->GetInteger());
                                                break;
                                            case kTypeFloat:
                                                sprintf(s, "%.2f", field->GetFloat());
                                                data = s;
                                                delete[] s;
                                                break;
                                            case kTypeChar:
                                                data = field->GetChars();
                                                break;
                                            default:
                                                break;
                                            }

                                            if (flag == false)
                                            {
                                                std::cout << data;
                                                flag = true;
                                            }
                                            else {
                                                std::cout << "\t" << data;
                                            }
                                        }
                                    }
                                }
                                std::cout << std::endl;
                            }
                        }
                    }
                    else {
                        std::cout << "minisql: Failed.\n";
                        return DB_FAILED;
                    }
                }

                if (selNum == 0)
                {
                    std::cout << "Empty set.\n";
                }
                else {
                    std::cout << "minisql: " << "The number of selected rows: " << selNum << ".\n";
                }
                return DB_SUCCESS;
            }
        }
        else {
            std::cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}



/// <summary>
/// 插入记录
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext* context) {
    if (current_db_ != "")
    {
        string tableName = ast->child_->val_;

        //先找table
        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
        {
            if (tableInfo->GetTableName() == tableName)
            {
                std::vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息


                std::vector<Field> fields;
                uint32_t index = 0;
                for (auto columnPt = ast->child_->next_->child_; columnPt != nullptr; columnPt = columnPt->next_, index++)
                {
                    if (index < columns.size())
                    {
                        Column* curCol = columns[index];
                        TypeId type = curCol->GetType();
                        string str;

                        if (columnPt->val_ != nullptr)
                        {
                            str = columnPt->val_;
                        }

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
                                    std::cout << "minisql[ERROR]: Input error.\n";
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
                                std::cout << "minisql[ERROR]: Syntax error.\n";
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
                                std::cout << "minisql[ERROR]: String too long.\n";
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
                                std::cout << "minisql[ERROR]: " << curCol->GetName() << " can't be nullable." << std::endl;
                                return DB_FAILED;
                            }
                            break;
                        default:
                            std::cout << "minisql[ERROR]: Syntax error.\n";
                            return DB_FAILED;
                            break;
                        }
                    }
                    else {
                        std::cout << "minisql[ERROR]: Input error.\n";
                        return DB_FAILED;
                    }
                }
                if (index == columns.size()) //列数匹配，继续
                {
                    Row row(fields);
                    std::map<string, Field*> valMap; //用于寻找对应field的map
                    for (uint32_t idx = 0; idx < columns.size(); idx++)
                    {
                        valMap.insert(std::pair<string, Field*>(columns[idx]->GetName(), row.GetField(idx)));
                    }
                    std::vector<IndexInfo*> indexes;
                    if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                    {
                        for (auto indexinfo : indexes)
                        {
                            if (indexinfo->GetIndexName()[0] == ';')
                            {
                                if (dynamic_cast<BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>*>(indexinfo->GetIndex())->IsEmpty())
                                {
                                    continue;
                                }
                                std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                std::vector<Field> indexFields;
                                for (auto col : cols)
                                {
                                    auto i = valMap.find(col->GetName());
                                    if (i != valMap.end())
                                    {
                                        indexFields.push_back(*(i->second));
                                    }
                                }
                                Row indexRow(indexFields);
                                std::vector<RowId> ids;
                                if (indexinfo->GetIndex()->ScanKey(indexRow, ids, nullptr) == DB_SUCCESS)
                                {
                                    std::cout << "minisql[ERROR]: Unique constraint.\n";
                                    return DB_FAILED;
                                }
                            }
                        }
                    }
                    else {
                        std::cout << "minisql[ERROR]: Failed.\n";
                        return DB_FAILED;
                    }

                    if (tableInfo->GetTableHeap()->InsertTuple(row, nullptr))
                    {
                        //添加index              
                        for (auto indexinfo : indexes)
                        {
                            std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                            std::vector<Field> indexFields;
                            for (auto col : cols)
                            {
                                auto i = valMap.find(col->GetName());
                                if (i != valMap.end())
                                {
                                    indexFields.push_back(*(i->second));
                                }
                            }
                            Row indexRow(indexFields);
                            uint32_t size = indexRow.GetSerializedSize(nullptr);
                            if (size > 32)
                            {
                                std::cout << "minisql: " << indexinfo->GetIndexName() << " has too long key, it has been deleted\n";
                                curDB->catalog_mgr_->DropIndex(tableName, indexinfo->GetIndexName());
                            }
                            else {
                                if (indexinfo->GetIndex()->InsertEntry(indexRow, row.GetRowId(), nullptr) == DB_SUCCESS)
                                {
                                }
                                else {
                                    std::cout << "minisql[ERROR]: Index " << indexinfo->GetIndexName() << " failed.\n";
                                    return DB_FAILED;
                                }
                            }
                        }
                        std::cout << "minisql: Insert successfully.\n";
                        return DB_SUCCESS;
                    }
                    else {
                        std::cout << "minisql[ERROR]: Insert failed.\n";
                        return DB_FAILED;
                    }
                }
                else {
                    std::cout << "minisql[ERROR]: Input error.\n";
                    return DB_FAILED;
                }
            }
        }
        else {
            std::cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}



/// <summary>
/// 删除
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext* context) {
    if (current_db_ != "")
    {
        string tableName = ast->child_->val_;
        uint32_t delNum = 0;
        
        TableInfo* tableInfo = TableInfo::Create(new SimpleMemHeap());
        if (curDB->catalog_mgr_->GetTable(tableName, tableInfo) == DB_SUCCESS) //找到这个名字了，继续
        {
            if (tableInfo->GetTableName() == tableName)
            {
                std::vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息
                std::map<string, TypeId> typeMap; //用于寻找对应type的id
                std::map<string, uint32_t> lengthMap;
                std::map<string, uint32_t> idxMap; //寻找对应列号
                for (uint32_t index = 0; index < columns.size(); index++)
                {
                    typeMap.insert(std::pair<string, TypeId>(columns[index]->GetName(), columns[index]->GetType()));
                    lengthMap.insert(std::pair<string, uint32_t>(columns[index]->GetName(), columns[index]->GetLength()));
                    idxMap.insert(std::pair<string, uint32_t>(columns[index]->GetName(), index));
                }

                //索引查找需要删除的
                if (ast->child_->next_ != nullptr) //索引部分，有子句
                {
                    std::vector<IndexInfo*> indexes;
                    if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                    {
                        if (indexes.size() != 0)
                        {
                            IndexInfo* indexinfoFinal = nullptr;
                            std::map<std::string, fieldCmp> indexFinal; //子句
                            std::map<std::string, fieldCmp> etcFinal; //子句
                            size_t etcSize = SIZE_MAX; //选etcsize最小的

                            //遍历所有index
                            for (auto indexinfo : indexes)
                            {
                                string indexName = indexinfo->GetIndexName();
                                if (indexName[0] == ';')
                                {
                                    continue;
                                }
                                //每个index
                                std::map<std::string, fieldCmp> indexParser;
                                std::map<std::string, fieldCmp> etcParser;
                                std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                std::set<string> colNameSet; //index列名集合
                                for (auto col : cols)
                                {
                                    string colName = col->GetName();
                                    colNameSet.insert(colName);
                                }
                                if (ClauseAndParser(typeMap, lengthMap, ast->child_->next_->child_, colNameSet, indexParser, etcParser))//解析子句成分
                                {
                                    if (colNameSet.size() == indexParser.size())
                                    {
                                        //符合index查询要求
                                        if (etcParser.size() < etcSize)
                                        {
                                            etcSize = etcParser.size();
                                            indexFinal = indexParser;
                                            etcFinal = etcParser;
                                            indexinfoFinal = indexinfo;
                                        }
                                    }
                                }
                            }

                            if (indexinfoFinal != nullptr)
                            {
                                 std::vector<Field> keyFields;
                                std::vector<Column*> keyCols = indexinfoFinal->GetIndexKeySchema()->GetColumns(); //正确顺序
                                for (auto keyCol : keyCols)
                                {
                                    auto i = indexFinal.find(keyCol->GetName());

                                    if (i != indexFinal.end())
                                    {
                                        keyFields.push_back(*(i->second.field));
                                    }
                                    else {
                                        std::cout << "minisql: Failed.\n";
                                        return DB_FAILED;
                                    }
                                }

                                Row keyRow(keyFields); //索引key

                                GenericKey<32> gKey;
                                gKey.SerializeFromKey(keyRow, indexinfoFinal->GetIndexKeySchema());

                                BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>* bpindex = dynamic_cast<BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>*>(indexinfoFinal->GetIndex());
                                if (!bpindex->IsEmpty())
                                {
                                    std::vector<RowId> ids;
                                    if (bpindex->ScanKey(keyRow, ids, nullptr) != DB_KEY_NOT_FOUND)
                                    {
                                        IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> goalIter = bpindex->GetBeginIterator(gKey);
                                        Row goalRow((*goalIter).second);
                                        if (tableInfo->GetTableHeap()->GetTuple(&goalRow, nullptr))
                                        {

                                        }
                                        else {
                                            std::cout << "minisql: Row Failed.\n";
                                            return DB_FAILED;
                                        }
                                        std::vector<Field*> goalFields = goalRow.GetFields();
                                        IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> endIter = goalIter;
                                        //模式选择
                                        int cmpState = -2; //-1 直接用tableiter 0完全等于 1大于iter范围 2小于iter范围
                                        for (auto& iter : indexFinal)
                                        {
                                            string cmp = iter.second.cmp;

                                            if (cmp == "<>")
                                            {
                                                cmpState = -1;
                                                break;
                                            }
                                            else if (cmp == "=")
                                            {
                                                if (cmpState == -2)
                                                {
                                                    cmpState = 0;
                                                }
                                            }
                                            else if (cmp == "<" || cmp == "<=")
                                            {
                                                if (cmpState == -2 || cmpState == 0)
                                                {
                                                    cmpState = 2;
                                                }
                                                else if (cmpState == 1)
                                                {
                                                    cmpState = -1;
                                                    break;
                                                }

                                            }
                                            else if (cmp == ">" || cmp == ">=")
                                            {
                                                if (cmpState == -2 || cmpState == 0)
                                                {
                                                    cmpState = 1;
                                                }
                                                else if (cmpState == 2)
                                                {
                                                    cmpState = -1;
                                                    break;
                                                }
                                            }
                                        }
                                        switch (cmpState)
                                        {
                                        case 0:
                                            if (RecordJudge(goalRow, etcFinal, idxMap))
                                            {
                                                //符合条件，可以删
                                                if (tableInfo->GetTableHeap()->MarkDelete(goalRow.GetRowId(), nullptr))
                                                {
                                                    delNum++;

                                                    //移除index
                                                    std::vector<IndexInfo*> indexes;
                                                    if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                                                    {
                                                        for (auto indexinfo : indexes)
                                                        {
                                                            std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                                            std::vector<Field> indexFields;
                                                            for (auto col : cols) //每个索引列
                                                            {
                                                                indexFields.push_back(*(goalFields[col->GetTableInd()]));
                                                            }
                                                            Row indexRow(indexFields);
                                                            if (indexinfo->GetIndex()->RemoveEntry(indexRow, goalRow.GetRowId(), nullptr) == DB_SUCCESS)
                                                            {
                                                            }
                                                            else {
                                                                std::cout << "minisql[ERROR]: Failed.\n";
                                                                return DB_FAILED;
                                                            }
                                                        }
                                                        tableInfo->GetTableHeap()->ApplyDelete(goalRow.GetRowId(), nullptr);
                                                    }
                                                    else {
                                                        std::cout << "minisql[ERROR]: Failed.\n";
                                                        return DB_FAILED;
                                                    }
                                                }
                                                else {
                                                    std::cout << "minisql[ERROR]: Insert failed.\n";
                                                    return DB_FAILED;
                                                }
                                            }
                                            std::cout << "minisql: " << "The number of deleted records: " << delNum << ".\n";
                                            return DB_SUCCESS;
                                            break;
                                        case 1:
                                            //大于范围
                                            for (auto idxIter = goalIter; idxIter != bpindex->GetEndIterator(); ++idxIter)
                                            {
                                                Row thisRow((*idxIter).second);
                                                if (tableInfo->GetTableHeap()->GetTuple(&thisRow, nullptr))
                                                {

                                                }
                                                else {
                                                    std::cout << "minisql: Row Failed.\n";
                                                    return DB_FAILED;
                                                }
                                                std::vector<Field*> fields = thisRow.GetFields();
                                                if (RecordJudge(thisRow, etcFinal, idxMap) && RecordJudge(thisRow, indexFinal, idxMap))
                                                {
                                                    //符合条件，可以删
                                                    if (tableInfo->GetTableHeap()->MarkDelete(thisRow.GetRowId(), nullptr))
                                                    {
                                                        delNum++;

                                                        //移除index
                                                        std::vector<IndexInfo*> indexes;
                                                        if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                                                        {
                                                            for (auto indexinfo : indexes)
                                                            {
                                                                std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                                                std::vector<Field> indexFields;
                                                                for (auto col : cols) //每个索引列
                                                                {
                                                                    indexFields.push_back(*(fields[col->GetTableInd()]));
                                                                }
                                                                Row indexRow(indexFields);
                                                                if (indexinfo->GetIndex()->RemoveEntry(indexRow, thisRow.GetRowId(), nullptr) == DB_SUCCESS)
                                                                {
                                                                }
                                                                else {
                                                                    std::cout << "minisql[ERROR]: Failed.\n";
                                                                    return DB_FAILED;
                                                                }
                                                            }
                                                            tableInfo->GetTableHeap()->ApplyDelete(thisRow.GetRowId(), nullptr);
                                                        }
                                                        else {
                                                            std::cout << "minisql[ERROR]: Failed.\n";
                                                            return DB_FAILED;
                                                        }
                                                    }
                                                    else {
                                                        std::cout << "minisql[ERROR]: Insert failed.\n";
                                                        return DB_FAILED;
                                                    }
                                                }
                                            }
                                            std::cout << "minisql: " << "The number of deleted records: " << delNum << ".\n";
                                            return DB_SUCCESS;
                                            break;
                                        case 2:
                                            //小于范围
                                            ++endIter;
                                            for (auto idxIter = bpindex->GetBeginIterator(); idxIter != endIter; ++idxIter)
                                            {
                                                Row thisRow((*idxIter).second);
                                                if (tableInfo->GetTableHeap()->GetTuple(&thisRow, nullptr))
                                                {

                                                }
                                                else {
                                                    std::cout << "minisql: Row Failed.\n";
                                                    return DB_FAILED;
                                                }
                                                std::vector<Field*> fields = thisRow.GetFields();
                                                if (RecordJudge(thisRow, etcFinal, idxMap) && RecordJudge(thisRow, indexFinal, idxMap))
                                                {
                                                    //符合条件，可以删
                                                    if (tableInfo->GetTableHeap()->MarkDelete(thisRow.GetRowId(), nullptr))
                                                    {
                                                        delNum++;

                                                        //移除index
                                                        std::vector<IndexInfo*> indexes;
                                                        if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                                                        {
                                                            for (auto indexinfo : indexes)
                                                            {
                                                                std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                                                std::vector<Field> indexFields;
                                                                for (auto col : cols) //每个索引列
                                                                {
                                                                    indexFields.push_back(*(fields[col->GetTableInd()]));
                                                                }
                                                                Row indexRow(indexFields);
                                                                if (indexinfo->GetIndex()->RemoveEntry(indexRow, thisRow.GetRowId(), nullptr) == DB_SUCCESS)
                                                                {
                                                                }
                                                                else {
                                                                    std::cout << "minisql[ERROR]: Failed.\n";
                                                                    return DB_FAILED;
                                                                }
                                                            }
                                                            tableInfo->GetTableHeap()->ApplyDelete(thisRow.GetRowId(), nullptr);
                                                        }
                                                        else {
                                                            std::cout << "minisql[ERROR]: Failed.\n";
                                                            return DB_FAILED;
                                                        }
                                                    }
                                                    else {
                                                        std::cout << "minisql[ERROR]: Insert failed.\n";
                                                        return DB_FAILED;
                                                    }
                                                }
                                            }
                                            std::cout << "minisql: " << "The number of deleted records: " << delNum << ".\n";
                                            return DB_SUCCESS;
                                            break;
                                        default:
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }



                //没有索引，用table迭代器遍历
                for (auto iter = tableInfo->GetTableHeap()->Begin(nullptr); iter != tableInfo->GetTableHeap()->End(); iter++)
                {
                    //遍历每一行
                    std::vector<Field*> fields = iter->GetFields();
                    RowId rowID = iter->GetRowId();
                    std::map<string, Field*> valMap; //用于寻找对应field的map
                    std::map<string, TypeId> typeMap; //用于寻找对应type的id
                    
                    //判断field和column大小是否相等
                    if (columns.size() == fields.size())
                    {
                        for (uint32_t index = 0; index < columns.size(); index++)
                        {
                            valMap.insert(std::pair<string, Field*>(columns[index]->GetName(), fields[index]));
                            typeMap.insert(std::pair<string, TypeId>(columns[index]->GetName(), columns[index]->GetType()));
                        }

                        //有没有子句问题
                        if (ast->child_->next_ == nullptr) //没有子句
                        {
                            if (tableInfo->GetTableHeap()->MarkDelete(rowID, nullptr))
                            {
                                delNum++;

                                //移除index
                                std::vector<IndexInfo*> indexes;
                                if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                                {
                                    for (auto indexinfo : indexes)
                                    {
                                        std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                        std::vector<Field> indexFields;
                                        for (auto col : cols)
                                        {
                                            auto i = valMap.find(col->GetName());
                                            if (i != valMap.end())
                                            {
                                                indexFields.push_back(*(i->second));
                                            }
                                        }
                                        Row indexRow(indexFields);
                                        if (indexinfo->GetIndex()->RemoveEntry(indexRow, rowID, nullptr) == DB_SUCCESS)
                                        {
                                        }
                                        else {
                                            std::cout << "minisql[ERROR]: Failed.\n";
                                            return DB_FAILED;
                                        }
                                    }
                                    tableInfo->GetTableHeap()->ApplyDelete(rowID, nullptr);
                                }
                                else {
                                    std::cout << "minisql[ERROR]: Failed.\n";
                                    return DB_FAILED;
                                }
                            }
                            else {
                                std::cout << "minisql[ERROR]: Insert failed.\n";
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

                                    //移除index
                                    std::vector<IndexInfo*> indexes;
                                    if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                                    {
                                        for (auto indexinfo : indexes)
                                        {
                                            std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                            std::vector<Field> indexFields;
                                            for (auto col : cols)
                                            {
                                                auto i = valMap.find(col->GetName());
                                                if (i != valMap.end())
                                                {
                                                    indexFields.push_back(*(i->second));
                                                }
                                            }
                                            Row indexRow(indexFields);
                                            if (indexinfo->GetIndex()->RemoveEntry(indexRow, rowID, nullptr) == DB_SUCCESS)
                                            {
                                            }
                                            else {
                                                std::cout << "minisql[ERROR]: Failed.\n";
                                                return DB_FAILED;
                                            }
                                        }
                                        tableInfo->GetTableHeap()->ApplyDelete(rowID, nullptr);
                                    }
                                    else {
                                        std::cout << "minisql[ERROR]: Failed.\n";
                                        return DB_FAILED;
                                    }
                                }
                                else {
                                    std::cout << "minisql[ERROR]: Insert failed.\n";
                                    return DB_FAILED;
                                }
                            }
                        }
                    }
                    else {
                        std::cout << "minisql: Failed.\n";
                        return DB_FAILED;
                    }
                }
                
                std::cout << "minisql: " << "The number of deleted records: " << delNum << ".\n";
                return DB_SUCCESS;
            }
        }
        else {
            std::cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}


/// <summary>
/// 更新
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext* context) {
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
                std::vector<Column*> columns = tableInfo->GetSchema()->GetColumns(); //获取表的所有列信息
                std::map<string, uint32_t> indexMap; //寻找对应index
                std::map<string, uint32_t> lengthMap;
                std::map<string, TypeId> typeMap; //用于寻找对应type的id

                

                //获取typeMap index与名字映射
                for (uint32_t index = 0; index < columns.size(); index++)
                {
                    indexMap.insert(std::pair<string, uint32_t>(columns[index]->GetName(), index));
                    lengthMap.insert(std::pair<string, uint32_t>(columns[index]->GetName(), columns[index]->GetLength()));
                    typeMap.insert(std::pair<string, TypeId>(columns[index]->GetName(), columns[index]->GetType()));
                }


                //初始化Field*数组
                std::vector<Field*> updateFields(columns.size(), nullptr); //装载的fields

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
                        
                        string str;
                        if (kNode->child_->next_->val_ != nullptr)
                        {
                            str = kNode->child_->next_->val_;
                        }
                        //根据不同类型装载field
                        switch (kNode->child_->next_->type_)
                        {
                        case kNodeNumber:
                            if (type == kTypeInt)
                            {
                                //原数整数，输入的数据不是整数报错
                                if (str.find('.') == str.npos)
                                {
                                    int32_t num = atoi(str.c_str());
                                    inField = new Field(type, num);
                                }
                                else {
                                    std::cout << "minisql[ERROR]: Input error.\n";
                                    return DB_FAILED;
                                }
                            }
                            else {
                                //原数小数，输入数据直接装进field，输入整数也当作小数
                                float num = atof(kNode->child_->next_->val_);
                                inField = new Field(type, num);
                            }
                            break;
                        case kNodeString:
                            inField = new Field(type, kNode->child_->next_->val_, uint32_t(str.size() + 1), true);
                            break;
                        default:
                            std::cout << "MiniSql: Failed.\n";
                            return DB_FAILED;
                            break;
                        }

                        updateFields[indexIt->second] = inField;
                    }
                    else {
                        std::cout << "minisql: No name.\n";
                        return DB_FAILED;
                    }
                }


                //索引查询需要更新的
                if (ast->child_->next_ != nullptr) //索引部分，有子句
                {
                    std::vector<IndexInfo*> indexes;
                    if (curDB->catalog_mgr_->GetTableIndexes(tableName, indexes) == DB_SUCCESS)
                    {
                        if (indexes.size() != 0)
                        {
                            IndexInfo* indexinfoFinal = nullptr;
                            std::map<std::string, fieldCmp> indexFinal; //子句
                            std::map<std::string, fieldCmp> etcFinal; //子句
                            size_t etcSize = SIZE_MAX; //选etcsize最小的

                            //遍历所有index
                            for (auto indexinfo : indexes)
                            {
                                string indexName = indexinfo->GetIndexName();
                                if (indexName[0] == ';')
                                {
                                    continue;
                                }
                                //每个index
                                std::map<std::string, fieldCmp> indexParser;
                                std::map<std::string, fieldCmp> etcParser;
                                std::vector<Column*> cols = indexinfo->GetIndexKeySchema()->GetColumns();
                                std::set<string> colNameSet; //index列名集合
                                for (auto col : cols)
                                {
                                    string colName = col->GetName();
                                    colNameSet.insert(colName);
                                }
                                if (ClauseAndParser(typeMap, lengthMap, ast->child_->next_->next_->child_, colNameSet, indexParser, etcParser))//解析子句成分
                                {
                                    if (colNameSet.size() == indexParser.size())
                                    {
                                        //符合index查询要求
                                        if (etcParser.size() < etcSize)
                                        {
                                            etcSize = etcParser.size();
                                            indexFinal = indexParser;
                                            etcFinal = etcParser;
                                            indexinfoFinal = indexinfo;
                                        }
                                    }
                                }
                            }

                            if (indexinfoFinal != nullptr)
                            {
                                std::vector<Field> keyFields;
                                std::vector<Column*> keyCols = indexinfoFinal->GetIndexKeySchema()->GetColumns(); //正确顺序
                                for (auto keyCol : keyCols)
                                {
                                    auto i = indexFinal.find(keyCol->GetName());

                                    if (i != indexFinal.end())
                                    {
                                        keyFields.push_back(*(i->second.field));
                                    }
                                    else {
                                        std::cout << "minisql: Failed.\n";
                                        return DB_FAILED;
                                    }
                                }

                                Row keyRow(keyFields); //索引key

                                GenericKey<32> gKey;
                                gKey.SerializeFromKey(keyRow, indexinfoFinal->GetIndexKeySchema());

                                BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>* bpindex = dynamic_cast<BPlusTreeIndex<GenericKey<32>, RowId, GenericComparator<32>>*>(indexinfoFinal->GetIndex());
                                if (!bpindex->IsEmpty())
                                {
                                    std::vector<RowId> ids;
                                    if (bpindex->ScanKey(keyRow, ids, nullptr) != DB_KEY_NOT_FOUND)
                                    {
                                        IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> goalIter = bpindex->GetBeginIterator(gKey);
                                        Row goalRow((*goalIter).second);
                                        if (tableInfo->GetTableHeap()->GetTuple(&goalRow, nullptr))
                                        {

                                        }
                                        else {
                                            std::cout << "minisql: Row Failed.\n";
                                            return DB_FAILED;
                                        }
                                        std::vector<Field*> goalFields = goalRow.GetFields();
                                        IndexIterator<GenericKey<32>, RowId, GenericComparator<32>> endIter = goalIter;
                                        //模式选择
                                        int cmpState = -2; //-1 直接用tableiter 0完全等于 1大于iter范围 2小于iter范围
                                        for (auto& iter : indexFinal)
                                        {
                                            string cmp = iter.second.cmp;

                                            if (cmp == "<>")
                                            {
                                                cmpState = -1;
                                                break;
                                            }
                                            else if (cmp == "=")
                                            {
                                                if (cmpState == -2)
                                                {
                                                    cmpState = 0;
                                                }
                                            }
                                            else if (cmp == "<" || cmp == "<=")
                                            {
                                                if (cmpState == -2 || cmpState == 0)
                                                {
                                                    cmpState = 2;
                                                }
                                                else if (cmpState == 1)
                                                {
                                                    cmpState = -1;
                                                    break;
                                                }

                                            }
                                            else if (cmp == ">" || cmp == ">=")
                                            {
                                                if (cmpState == -2 || cmpState == 0)
                                                {
                                                    cmpState = 1;
                                                }
                                                else if (cmpState == 2)
                                                {
                                                    cmpState = -1;
                                                    break;
                                                }
                                            }
                                        }
                                        switch (cmpState)
                                        {
                                        case 0:
                                            if (RecordJudge(goalRow, etcFinal, indexMap))
                                            {
                                                //符合条件，可以更新
                                                std::vector<Field> loadField; //最后加载到row的field
                                                std::vector<Field*> fields = goalRow.GetFields();
                                                for (size_t i = 0; i < fields.size(); i++)
                                                {
                                                    if (updateFields[i] != nullptr)
                                                    {
                                                        fields[i] = updateFields[i];
                                                    }

                                                    loadField.push_back(*fields[i]);
                                                }
                                                Row loadRow(loadField);
                                                if (tableInfo->GetTableHeap()->UpdateTuple(loadRow, goalRow.GetRowId(), nullptr))
                                                {
                                                    updateNum++;
                                                }
                                                else {
                                                    std::cout << "minisql[ERROR]: Insert failed.\n";
                                                    return DB_FAILED;
                                                }
                                            }
                                            std::cout << "minisql: " << "The number of updated rows: " << updateNum << ".\n";
                                            return DB_SUCCESS;
                                            break;
                                        case 1:
                                            //大于范围
                                            for (auto idxIter = goalIter; idxIter != bpindex->GetEndIterator(); ++idxIter)
                                            {
                                                Row thisRow((*idxIter).second);
                                                if (tableInfo->GetTableHeap()->GetTuple(&thisRow, nullptr))
                                                {

                                                }
                                                else {
                                                    std::cout << "minisql: Row Failed.\n";
                                                    return DB_FAILED;
                                                }
                                                if (RecordJudge(thisRow, etcFinal, indexMap) && RecordJudge(thisRow, indexFinal, indexMap))
                                                {
                                                    //符合条件，可以更新
                                                    std::vector<Field> loadField; //最后加载到row的field
                                                    std::vector<Field*> fields = thisRow.GetFields();
                                                    for (size_t i = 0; i < fields.size(); i++)
                                                    {
                                                        if (updateFields[i] != nullptr)
                                                        {
                                                            fields[i] = updateFields[i];
                                                        }

                                                        loadField.push_back(*fields[i]);
                                                    }
                                                    Row loadRow(loadField);
                                                    if (tableInfo->GetTableHeap()->UpdateTuple(loadRow, thisRow.GetRowId(), nullptr))
                                                    {
                                                        updateNum++;
                                                    }
                                                    else {
                                                        std::cout << "minisql[ERROR]: Insert failed.\n";
                                                        return DB_FAILED;
                                                    }
                                                }
                                            }
                                            std::cout << "minisql: " << "The number of updated rows: " << updateNum << ".\n";
                                            return DB_SUCCESS;
                                            break;
                                        case 2:
                                            //小于范围
                                            ++endIter;
                                            for (auto idxIter = bpindex->GetBeginIterator(); idxIter != endIter; ++idxIter)
                                            {
                                                Row thisRow((*idxIter).second);
                                                if (tableInfo->GetTableHeap()->GetTuple(&thisRow, nullptr))
                                                {

                                                }
                                                else {
                                                    std::cout << "minisql: Row Failed.\n";
                                                    return DB_FAILED;
                                                }
                                                if (RecordJudge(thisRow, etcFinal, indexMap) && RecordJudge(thisRow, indexFinal, indexMap))
                                                {
                                                    //符合条件，可以更新
                                                    std::vector<Field> loadField; //最后加载到row的field
                                                    std::vector<Field*> fields = thisRow.GetFields();
                                                    for (size_t i = 0; i < fields.size(); i++)
                                                    {
                                                        if (updateFields[i] != nullptr)
                                                        {
                                                            fields[i] = updateFields[i];
                                                        }

                                                        loadField.push_back(*fields[i]);
                                                    }
                                                    Row loadRow(loadField);
                                                    if (tableInfo->GetTableHeap()->UpdateTuple(loadRow, thisRow.GetRowId(), nullptr))
                                                    {
                                                        updateNum++;
                                                    }
                                                    else {
                                                        std::cout << "minisql[ERROR]: Insert failed.\n";
                                                        return DB_FAILED;
                                                    }
                                                }
                                            }
                                            std::cout << "minisql: " << "The number of updated rows: " << updateNum << ".\n";
                                            return DB_SUCCESS;
                                            break;
                                        default:
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }




                //没有索引
                for (auto iter = tableInfo->GetTableHeap()->Begin(nullptr); iter != tableInfo->GetTableHeap()->End(); iter++)
                {
                    //遍历每一行
                    std::vector<Field*> fields = iter->GetFields();
                    RowId rowID = iter->GetRowId();
                    std::map<string, Field*> valMap; //用于寻找对应field的map
                    std::vector<Field> loadField; //最后加载到row的field

                    //判断field和column大小是否相等
                    if (columns.size() == fields.size())
                    {
                        for (uint32_t index = 0; index < columns.size(); index++)
                        {
                            valMap.insert(std::pair<string, Field*>(columns[index]->GetName(), fields[index]));
                        }

                        //有没有子句问题
                        if (ast->child_->next_->next_ == nullptr) //没有子句
                        {
                            //没有子句 直接更新
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
                            std::cout << "minisql: Failed.\n";
                            return DB_FAILED;
                        }
                    }
                    else {
                        std::cout << "minisql: Failed.\n";
                        return DB_FAILED;
                    }
                }

                std::cout << "minisql: " << "The number of updated rows: " << updateNum << ".\n";
                return DB_SUCCESS;
            }
        }
        else {
            std::cout << "minisql: No table.\n";
            return DB_FAILED;
        }
    }
    else {
        std::cout << "minisql: No database selected.\n";
        return DB_FAILED;
    }
    return DB_FAILED;
}








dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext* context) {
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext* context) {
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext* context) {
    return DB_FAILED;
}

/// <summary>
/// 执行规定的文件，把文件读取后重新执行指令
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext* context) {
    if (ast->type_ == kNodeExecFile)
    {
        string fileName = ast->child_->val_;

        //读文件
        std::ifstream fileCMD(fileName);
        if (!fileCMD.is_open())
        {
            std::cout << "minisql: No such file." << std::endl;
            return DB_FAILED;
        }
        const int buf_size = 1024;
        char cmd[buf_size];

        while (1) {
            FileCommand(cmd, buf_size, fileCMD);
            if (fileCMD.eof())
            {
                break;
            }
            
            std::cout << cmd << std::endl;

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


            // clean memory after parse
            MinisqlParserFinish();
            yy_delete_buffer(bp);
            yylex_destroy();

                      // 在脚本中 quit，直接退到命令行
            if (fileContext.flag_quit_) 
            {
                std::cout << fileName << ": Bye!" << std::endl;
                break;
            }
            
        }
        fileCMD.close();

        return DB_SUCCESS;
    }
    else {
        return DB_FAILED;
    }
    return DB_FAILED;
}

/// <summary>
/// 负责对脚本的读取
/// </summary>
/// <param name="input"></param>
/// <param name="len"></param>
/// <param name="in"></param>
void ExecuteEngine::FileCommand(char* input, const int len, std::ifstream& in) {
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
    input[i] = ch;    
}


/// <summary>
/// 退出指令，直接退出即可
/// </summary>
/// <param name="ast"></param>
/// <param name="context"></param>
/// <returns></returns>
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext* context) {
    ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
    context->flag_quit_ = true;
    return DB_SUCCESS;
}
