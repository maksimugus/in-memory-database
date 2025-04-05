#include <chrono>
#include <iomanip>
#include <iostream>
#include <fstream>

#include <ranges>
#include <unordered_map>
#include <vector>
#include <variant>

#include "../Parser/sql_parser.h"

class MyMonostate : public std::monostate {
 public:
  friend std::ostream& operator<<(std::ostream& stream, const MyMonostate&);
};

using Value = std::variant<MyMonostate, int, double, float, bool, std::string>;

class Column {
 public:
  Column() = default;
  explicit Column(DataType type, size_t max_len, bool can_be_null);
  void SetNotNull(bool status);
  void SetIsPrimary(bool is_primary);
  const Value& operator[](size_t id) const;
  size_t max_len_of_value() const;
  DataType type() const;
  size_t size() const;
  void PushValue(const Value& value);
  void EmplaceValue(const std::string& value);
  Column Select(const std::vector<size_t>& idx);
  void Update(const std::vector<size_t>& idx, const std::string& value);
  void Delete(const std::vector<size_t>& idx);
  void DeleteAll();
  void GetData(std::ofstream& f) const;
  void SetData(std::ifstream& f);
 private:
  DataType type_;
  size_t max_len_of_value_ = 0;
  bool is_primary_ = false;
  bool not_null_ = true;
  std::vector<Value> values_;
};

class Table {
 public:
  Table() = default;
  void SetPrimaryKey(const std::string& primary_key);
  friend std::ostream& operator<<(std::ostream& stream, const Table& response);
  const Column& operator[](const std::string& key);
  void CreateColumn(const std::tuple<std::string, DataType, size_t, bool>& info);
  void AddColumn(const std::pair<std::string, Column>& column);
  void CreateRow(std::unordered_map<std::string, std::string>& info);
  Table Select(const std::vector<std::string>& columns, const std::vector<Token>& filters = std::vector<Token>());
  Table Join(Table& table, const Column& column1, const Column& column2, bool is_inner);
  void Update(const std::unordered_map<std::string, std::string>& values, const std::vector<Token>& filters);
  void Delete(const std::vector<Token>& filters);
  void DeleteAll();
  bool ContainsColumn(const std::string& column) const;
  void GetData(std::ofstream& f) const;
  void SetData(std::ifstream& f);
 private:
  bool Compare(TokenType op, const Value& a, const Value& b);
  bool Check(const std::vector<Token>& filters, size_t row);
  std::unordered_map<std::string, Column> columns_;
  size_t n_rows_ = 0;
};

class Response {
 public:
  Response() = default;
  explicit Response(const std::string& msg);
  explicit Response(const Table& table);
  friend std::ostream& operator<<(std::ostream& stream, const Response& response);
 private:
  enum Type {
    kMessage,
    kTable
  };
  Type type_;
  std::variant<std::string, Table> data_;
};

class Database {
 public:
  Database() = default;
  Response Execute(const std::string& query);
  void Save(const std::string& file_name);
  void Open(const std::string& file_name);
 private:
  std::unordered_map<std::string, Table> tables_;
  Response CreateTable(const SerializerForCreate& info);
  Response DropTable(const SerializerForDrop& info);
  Response Insert(SerializerForInsert& info);
  Response Select(SerializerForSelect& info);
  Response Update(const SerializerForUpdate& info);
  Response Delete(const SerializerForDelete& info);
};

Value Cast(const std::string& value, DataType type);