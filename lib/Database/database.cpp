#include "database.h"

void Table::CreateColumn(const std::tuple<std::string, DataType, size_t, bool>& info) {
  columns_.emplace(std::get<0>(info),
                   Column{std::get<1>(info), std::get<2>(info), false});
}

void Table::SetPrimaryKey(const std::string& primary_key) {
  columns_[primary_key].SetNotNull(false);
  columns_[primary_key].SetIsPrimary(true);
}

void Table::CreateRow(std::unordered_map<std::string, std::string>& info) {
  for (auto& p : columns_) {
    if (info.contains(p.first)) {
      p.second.EmplaceValue(info[p.first]);
    } else {
      p.second.PushValue({});
    }
  }
  ++n_rows_;
}

std::ostream& operator<<(std::ostream& stream, const Table& table) {
  for (const auto& column : table.columns_) {
    stream << std::setw(std::max(column.second.max_len_of_value(), column.first.size() + 3))
           << std::left << column.first;
  }
  stream << '\n';
  for (size_t i = 0; i < table.n_rows_; ++i) {
    for (const auto& column : table.columns_) {
      std::visit(
          [&stream, &column](auto&& arg) {
            stream << std::setw(std::max(column.second.max_len_of_value(), column.first.size() + 3))
                   << std::left << arg;
          }, column.second[i]
      );
    }
    stream << '\n';
  }
  return stream;
}

void Column::SetNotNull(bool status) {
  not_null_ = status;
}

void Column::SetIsPrimary(bool is_primary) {
  is_primary_ = is_primary;
}

Column::Column(DataType type, size_t max_len, bool can_be_null) : type_(type) {
  if (max_len != 0) {
    max_len_of_value_ = max_len;
  } else {
    switch (type) {
      case kInt:
      case kDouble:
      case kFloat:
        max_len_of_value_ = 10;
        break;
      case kBool:
        max_len_of_value_ = 5;
        break;
      default:
        break;
    }
  }
}

void Column::EmplaceValue(const std::string& value) {
  if (value.size() > max_len_of_value_) {
    throw std::logic_error("Invalid value");
  }
  Value v;
  if (value == "NULL") {
    if (!not_null_) {
      throw std::logic_error("Invalid value");
    }
  } else {
    v = Cast(value, type_);
    if (is_primary_) {
      for (const auto& w : values_) {
        if (w == v) {
          throw std::logic_error(" Primary key '" + value + "' already exists");
        }
      }
    }
  }
  values_.push_back(v);
}

DataType Column::type() const {
  return type_;
}

Column Column::Select(const std::vector<size_t>& idx) {
  Column res;
  for (const auto& i : idx) {
    res.values_.push_back(values_[i]);
  }
  res.max_len_of_value_ = max_len_of_value_;
  return res;
}

const Value& Column::operator[](size_t id) const {
  return values_[id];
}

size_t Column::max_len_of_value() const {
  return max_len_of_value_;
}

void Column::Update(const std::vector<size_t>& idx, const std::string& value) {
  Value v;
  if (value != "NULL") {
    v = Cast(value, type_);
  }
  for (const auto& i : idx) {
    values_[i] = v;
  }
}

void Column::Delete(const std::vector<size_t>& idx) {
  for (const auto& i : std::ranges::reverse_view(idx)) {
    values_.erase(values_.begin() + i);
  }
}

void Column::DeleteAll() {
  values_.clear();
}

size_t Column::size() const {
  return values_.size();
}

void Column::PushValue(const Value& value) {
  values_.push_back(value);
}

void Column::GetData(std::ofstream& f) const {
  f << type_ << '\t' << max_len_of_value_ << '\t' << is_primary_
    << '\t' << not_null_ << '\t' << values_.size() << '\t';
  for (const auto& v : values_) {
    std::visit([&f](auto&& arg) { f << arg << '\t'; }, v);
  }
  f << '\n';
}

void Column::SetData(std::ifstream& f) {
  int dt;
  f >> dt;
  switch (dt) {
    case 0:
      type_ = kInt;
      break;
    case 1:
      type_ = kDouble;
      break;
    case 2:
      type_ = kFloat;
      break;
    case 3:
      type_ = kBool;
      break;
    case 4:
      type_ = kVarchar;
      break;
  }
  f >> max_len_of_value_ >> is_primary_ >> not_null_;
  size_t n;
  f >> n;
  for (size_t i = 0; i < n; ++i) {
    std::string buf;
    f >> buf;
    EmplaceValue(buf);
  }
}

void Database::Save(const std::string& file_name) {
  std::ofstream f("..\\..\\db_states\\" + file_name + ".tsv", std::ios::binary);
  f << tables_.size() << '\n';
  for (const auto& t : tables_) {
    f << t.first << '\n';
    t.second.GetData(f);
  }
}

void Database::Open(const std::string& file_name) {
  tables_.clear();
  std::ifstream f("..\\..\\db_states\\" + file_name + ".tsv", std::ios::binary);
  size_t n;
  f >> n;
  for (size_t i = 0; i < n; ++i) {
    std::string name;
    f >> name;
    tables_.emplace(name, Table());
    tables_[name].SetData(f);
  }
}

Response Database::Execute(const std::string& query) {
  Response r;
  Query q = SqlParser(query).Parse();
  switch (q.query_type) {
    case kCreate:
      r = CreateTable(std::get<SerializerForCreate>(q.serializer));
      break;
    case kDrop:
      r = DropTable(std::get<SerializerForDrop>(q.serializer));
      break;
    case kInsert:
      r = Insert(std::get<SerializerForInsert>(q.serializer));
      break;
    case kSelect:
      r = Select(std::get<SerializerForSelect>(q.serializer));
      break;
    case kUpdate:
      r = Update(std::get<SerializerForUpdate>(q.serializer));
      break;
    case kDelete:
      r = Delete(std::get<SerializerForDelete>(q.serializer));
      break;
    default:
      break;
  }
  return r;
}

Response Database::CreateTable(const SerializerForCreate& info) {
  Table table;
  for (const auto& column : info.table_columns) {
    table.CreateColumn(column);
  }
  table.SetPrimaryKey(std::get<0>(info.table_columns[info.primary_key]));
  tables_.emplace(info.table_name, table);
  return Response("Table is successfully created");
}

Response Database::DropTable(const SerializerForDrop& info) {
  tables_.erase(info.table_name);
  return Response("Table '" + info.table_name + "' was succesfully dropped");
}

Response Database::Insert(SerializerForInsert& info) {
  tables_[info.table_name].CreateRow(info.data);
  return Response("Information is successfully inserted");
}

Response Database::Select(SerializerForSelect& info) {
  if (!info.unique_columns.empty()) {
    for (const auto& c : info.unique_columns) {
      bool exists = false;
      for (const auto& t : tables_) {
        if (!exists) {
          if (t.second.ContainsColumn(c)) {
            exists = true;
            if (t.first == info.table_name1) {
              info.columns1.push_back(c);
            } else if (t.first == info.table_name2) {
              info.columns2.push_back(c);
            } else {
              throw std::logic_error("Invalid query");
            }
          }
        } else {
          throw std::logic_error("Ambiguous column selection");
        }
      }
    }
    info.unique_columns.clear();
  }
  if (info.all_table) {
    return Response(tables_[info.table_name1]);
  } else {
    if (!tables_.contains(info.table_name1)) {
      throw std::logic_error("No table with name '" + info.table_name1 + "'");
    }
    auto t1 = tables_[info.table_name1].Select(info.columns1, info.filters);
    if (!info.is_join) {
      return Response(t1);
    }
    if (!tables_.contains(info.table_name2)) {
      throw std::logic_error("No table with name '" + info.table_name2 + "'");
    }
    auto t2 = tables_[info.table_name2].Select(info.columns2);
    Column c1, c2;
    if (tables_[info.table_name1].ContainsColumn(info.join_columns.first) &&
        tables_[info.table_name2].ContainsColumn(info.join_columns.second)) {
      c1 = tables_[info.table_name1][info.join_columns.first];
      c2 = tables_[info.table_name2][info.join_columns.second];
    } else if (tables_[info.table_name1].ContainsColumn(info.join_columns.second) &&
        tables_[info.table_name2].ContainsColumn(info.join_columns.first)) {
      c1 = tables_[info.table_name1][info.join_columns.second];
      c2 = tables_[info.table_name2][info.join_columns.first];
    } else {
      throw std::logic_error("No column with given name");
    }
    switch (info.join_type) {
      case kInner:
        return Response(t1.Join(t2, c1, c2, true));
      case kLeft:
        return Response(t1.Join(t2, c1, c2, false));
      case kRight:
        return Response(t2.Join(t1, c2, c1, false));
    }
  }
  return Response();
}

Response Database::Update(const SerializerForUpdate& info) {
  if (!tables_.contains(info.table_name)) {
    throw std::logic_error("No table with given name");
  }
  tables_[info.table_name].Update(info.values, info.filters);
  return Response("Information was successfully updated");
}

Response Database::Delete(const SerializerForDelete& info) {
  if (!tables_.contains(info.table_name)) {
    throw std::logic_error("No table with given name");
  }
  if (info.all_table) {
    tables_[info.table_name].DeleteAll();
  } else {
    tables_[info.table_name].Delete(info.filters);
  }
  return Response("Information was successfully deleted");
}

Response::Response(const std::string& msg) : data_(msg), type_(kMessage) {}

Response::Response(const Table& table) : data_(table), type_(kTable) {}

std::ostream& operator<<(std::ostream& stream, const Response& response) {
  switch (response.type_) {
    case Response::kMessage:
      stream << std::get<std::string>(response.data_);
      break;
    case Response::kTable:
      stream << std::get<Table>(response.data_);
      break;
    default:
      break;
  }
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const MyMonostate&) {
  stream << "NULL";
  return stream;
}

Table Table::Select(const std::vector<std::string>& columns,
                    const std::vector<Token>& filters) {
  Table result;
  std::vector<size_t> sat_rows;
  bool all_rows = false;
  if (!filters.empty()) {
    for (size_t i = 0; i < n_rows_; ++i) {
      if (Check(filters, i)) {
        sat_rows.push_back(i);
      }
    }
    result.n_rows_ = sat_rows.size();
  } else {
    all_rows = true;
    result.n_rows_ = n_rows_;
  }
  for (const auto& c : columns) {
    if (!columns_.contains(c)) {
      throw std::logic_error("No column with given name");
    }
    if (all_rows) {
      result.columns_.emplace(c, columns_[c]);
    } else {
      result.columns_.emplace(c, columns_[c].Select(sat_rows));
    }
  }
  return result;
}

bool Table::Check(const std::vector<Token>& filters, size_t row) {
  std::stack<Token> stack; // values
  Token t1, t2;
  Value a, b;
  for (const auto& f : filters) {
    switch (f.type) {
      case kVar:
      case kConst:
        stack.emplace(f);
        break;
      case kEquals:
      case kNotEquals:
      case kGreater:
      case kLess:
      case kNotGreater:
      case kNotLess:
      case kOr:
      case kAnd:
        t2 = stack.top();
        stack.pop();
        t1 = stack.top();
        stack.pop();
        if (t1.type == kVar) {
          a = columns_[t1.value][row];
        } else if (t1.type == kConst && t2.type == kVar) {
          a = Cast(t1.value, columns_[t2.value].type());
        } else {
          a = Cast(t1.value, kBool);
        }
        if (t2.type == kVar) {
          b = columns_[t2.value][row];
        } else if (t2.type == kConst && t1.type == kVar) {
          b = Cast(t2.value, columns_[t1.value].type());
        } else {
          b = Cast(t2.value, kBool);
        }
        stack.emplace(kRes, std::to_string(Compare(f.type, a, b)));
        break;
      default:
        break;
    }
  }
  return std::get<bool>(Cast(stack.top().value, kBool));
}

bool Table::Compare(TokenType op, const Value& a, const Value& b) {
  switch (op) {
    case kEquals:
      return a == b;
    case kNotEquals:
      return a != b;
    case kGreater:
      return a > b;
    case kLess:
      return a < b;
    case kNotGreater:
      return a <= b;
    case kNotLess:
      return a >= b;
    case kOr:
      return std::get<bool>(a) || std::get<bool>(b);
    case kAnd:
      return std::get<bool>(a) && std::get<bool>(b);
    default:
      throw std::logic_error("Invalid operation");
  }
}

void Table::Update(const std::unordered_map<std::string, std::string>& values,
                   const std::vector<Token>& filters) {
  std::vector<size_t> sat_rows;
  for (size_t i = 0; i < n_rows_; ++i) {
    if (Check(filters, i)) {
      sat_rows.push_back(i);
    }
  }
  for (const auto& p : values) {
    if (!columns_.contains(p.first)) {
      throw std::logic_error("No column with given name");
    }
    columns_[p.first].Update(sat_rows, p.second);
  }
}

void Table::Delete(const std::vector<Token>& filters) {
  std::vector<size_t> sat_rows;
  for (size_t i = 0; i < n_rows_; ++i) {
    if (Check(filters, i)) {
      sat_rows.push_back(i);
    }
  }
  for (auto& p : columns_) {
    p.second.Delete(sat_rows);
  }
  n_rows_ -= sat_rows.size();
}

void Table::DeleteAll() {
  for (auto& c : columns_) {
    c.second.DeleteAll();
  }
  n_rows_ = 0;
}

bool Table::ContainsColumn(const std::string& column) const {
  return columns_.contains(column);
}

Table Table::Join(Table& table, const Column& column1, const Column& column2, bool is_inner) {
  Table res;

  for (const auto& c : columns_) {
    res.AddColumn(c);
  }

  for (const auto& c : table.columns_) {
    res.AddColumn(c);
  }

  for (size_t i = 0; i < column1.size(); ++i) {
    bool found = false;
    size_t j = 0;
    for (; j < column2.size(); ++j) {
      if (column1[i] == column2[j]) {
        found = true;
        break;
      }
    }
    if (found || !is_inner) {
      for (const auto& c : columns_) {
        res.columns_[c.first].PushValue(c.second[i]);
      }
      for (const auto& c : table.columns_) {
        if (!found) {
          res.columns_[c.first].PushValue({});
        } else {
          res.columns_[c.first].PushValue(c.second[j]);
        }
      }
      ++res.n_rows_;
    }
  }

  return res;
}
const Column& Table::operator[](const std::string& key) {
  return columns_[key];
}

void Table::GetData(std::ofstream& f) const {
  f << columns_.size() << '\t' << n_rows_ << '\n';
  for (const auto& c : columns_) {
    f << c.first << '\t';
    c.second.GetData(f);
  }
}

void Table::SetData(std::ifstream& f) {
  size_t n;
  f >> n;
  f >> n_rows_;
  for (size_t i = 0; i < n; ++i) {
    std::string name;
    f >> name;
    columns_.emplace(name, Column());
    columns_[name].SetData(f);
  }
}

void Table::AddColumn(const std::pair<std::string, Column>& column) {
  columns_.emplace(column);
}

Value Cast(const std::string& value, DataType type) {
  Value res;
  switch (type) {
    case kInt:
      try {
        res = std::stoi(value);
      } catch (...) {
        throw std::logic_error("Invalid value");
      }
      break;
    case kDouble:
      try {
        res = std::stod(value);
      } catch (...) {
        throw std::logic_error("Invalid value");
      }
      break;
    case kFloat:
      try {
        res = std::stof(value);
      } catch (...) {
        throw std::logic_error("Invalid value");
      }
      break;
    case kBool:
      bool tmp;
      std::istringstream(value) >> std::noboolalpha >> tmp;
      res = tmp;
      break;
    case kVarchar:
      res = value;
      break;
  }
  return res;
}
