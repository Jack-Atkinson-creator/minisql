#ifndef MINISQL_EXECUTE_ENGINE_H
#define MINISQL_EXECUTE_ENGINE_H

#include "common/dberr.h"
#include "common/instance.h"
#include "transaction/transaction.h"
#include <iostream>
#include <string>
#include <map>
#include <set>
#include <unordered_map>


extern "C" {
#include "parser/parser.h"
};


/**
 * ExecuteContext stores all the context necessary to run in the execute engine
 * This struct is implemented by student self for necessary.
 *
 * eg: transaction info, execute result...
 */
struct ExecuteContext {
  bool flag_quit_{false};
  Transaction *txn_{nullptr};
};

/**
 * ExecuteEngine
 */
class ExecuteEngine {
public:
  ExecuteEngine();

  ~ExecuteEngine() {
    for (auto it : dbs_) {
      delete it.second;
    }
  }

  /**
   * executor interface
   */
  dberr_t Execute(pSyntaxNode ast, ExecuteContext *context);

private:
  dberr_t ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteSelect(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteInsert(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDelete(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteQuit(pSyntaxNode ast, ExecuteContext *context);

private:
    struct fieldCmp {
        Field* field;
        std::string cmp;
    };
    std::unordered_map<std::string, DBStorageEngine*> dbs_;  /** all opened databases */
    std::string current_db_;  /** current database */
    DBStorageEngine* curDB; //当前的指针



    std::string dbPath; //db文件放的地方
    void DBIntialize();
    void FileCommand(char* input, const int len, std::ifstream& in);
    bool ClauseAnalysis(std::map<std::string, Field*>& valMap, std::map<std::string, TypeId>& typeMap, pSyntaxNode kNode);
    bool ClauseAndParser(std::map<std::string, TypeId>& typeMap, std::map<std::string, uint32_t>& lengthMap, pSyntaxNode kNode, std::set<std::string>& colNameSet, std::map<std::string, fieldCmp>& parserIndexRes, std::map<std::string, fieldCmp>& parserEtcRes);
    bool RecordJudge(Row& row, std::map<std::string, fieldCmp>& parser, std::map<std::string, uint32_t>& idxMap);
};

#endif //MINISQL_EXECUTE_ENGINE_H
