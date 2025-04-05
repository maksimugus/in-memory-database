#pragma once

#include <tuple>
#include <variant>
#include <vector>
#include <queue>
#include <unordered_map>

#include "Base/base_parser.h"

enum QueryType {
  kCreate,
  kDrop,
  kInsert,
  kSelect,
  kUpdate,
  kDelete
};

enum DataType {
  kInt,
  kDouble,
  kFloat,
  kBool,
  kVarchar
};

enum JoinType {
  kInner,
  kLeft,
  kRight
};

struct SerializerForCreate {
  std::string table_name;
  std::vector<std::tuple<std::string, DataType, size_t, bool>> table_columns;
  size_t primary_key;
};

struct SerializerForDrop {
  std::string table_name;
};

struct SerializerForInsert {
  std::string table_name;
  std::unordered_map<std::string, std::string> data;
};

struct SerializerForSelect {
  std::string table_name1;
  std::string table_name2;
  std::vector<std::string> columns1;
  std::vector<std::string> columns2;
  std::vector<std::string> unique_columns;
  std::vector<Token> filters;
  bool all_table = false;
  bool is_join = false;
  std::pair<std::string, std::string> join_columns;
  JoinType join_type = kInner;
};

struct SerializerForUpdate {
  std::string table_name;
  std::unordered_map<std::string, std::string> values;
  std::vector<Token> filters;
};

struct SerializerForDelete {
  std::string table_name;
  std::vector<Token> filters;
  bool all_table = true;
};

struct Query {
  QueryType query_type;
  std::variant<SerializerForCreate, SerializerForDrop,
               SerializerForInsert, SerializerForSelect,
               SerializerForUpdate, SerializerForDelete> serializer;
};

class SqlParser : public BaseParser {
 public:
  explicit SqlParser(const std::string& query);
  Query Parse();
 private:
  SerializerForCreate ParseCreate();
  SerializerForDrop ParseDrop();
  SerializerForInsert ParseInsert();
  SerializerForSelect ParseSelect();
  SerializerForUpdate ParseUpdate();
  SerializerForDelete ParseDelete();
  void ParseJoin(SerializerForSelect& serializer);
};

