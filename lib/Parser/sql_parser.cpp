#include "sql_parser.h"

SqlParser::SqlParser(const std::string& query) : BaseParser(query) {}

Query SqlParser::Parse() {
  Query q;
  SkipWhitespace();
  if (Take('C')) {
    q = {kCreate, ParseCreate()};
  } else if (Take('I')) {
    q = {kInsert, ParseInsert()};
  } else if (Take('S')) {
    q = {kSelect, ParseSelect()};
  } else if (Take('U')) {
    q = {kUpdate, ParseUpdate()};
  } else if (Take('D')) {
    if (Take('E')) {
      q = {kDelete, ParseDelete()};
    } else if (Take('R')) {
      q = {kDrop, ParseDrop()};
    }
  } else {
    throw Error("Unsupported query");
  }
  CheckEof();
  return q;
}

SerializerForCreate SqlParser::ParseCreate() {
  Expect("REATE");
  SkipWhitespace();
  Expect("TABLE");
  SkipWhitespace();

  SerializerForCreate serializer;
  serializer.table_name = TakeWord();
  SkipWhitespace();
  Expect('(');
  bool primary_is_set = false;
  while (!Eof() && !Test(')')) {
    SkipWhitespace();
    std::string name = TakeWord();
    SkipWhitespace();
    DataType type;
    size_t len = 0;
    if (Take('I')) {
      Expect("NT");
      type = kInt;
    } else if (Take('B')) {
      Expect("OOL");
      type = kBool;
    } else if (Take('D')) {
      Expect("OUBLE");
      type = kDouble;
    } else if (Take('F')) {
      Expect("LOAT");
      type = kFloat;
    } else if (Take('V')) {
      std::string buf;
      Expect("ARCHAR");
      type = kVarchar;
      SkipWhitespace();
      Expect('(');
      SkipWhitespace();
      while (!Eof() && !Test(')') && !std::isspace(cur_)) {
        buf += Take();
      }
      if (!buf.empty()) {
        len = std::stoull(buf);
      } else {
        throw Error("Varchar size is not set");
      }
      SkipWhitespace();
      Expect(')');
    } else {
      throw Error("Invalid data type");
    }
    SkipWhitespace();
    if (!primary_is_set && Take('P')) {
      Expect("RIMARY");
      SkipWhitespace();
      Expect("KEY");
      SkipWhitespace();
      serializer.primary_key = serializer.table_columns.size();
      primary_is_set = true;
    }
    bool not_null = false;
    if (Take('N')) {
      Expect("OT");
      SkipWhitespace();
      Expect("NULL");
      not_null = true;
      SkipWhitespace();
    }
    if (!Test(')')) {
      Expect(',');
    }
    serializer.table_columns.emplace_back(name, type, len, not_null);
  }
  Expect(')');

  if (!primary_is_set) {
    throw std::logic_error("Primary key is not set");
  }

  SkipWhitespace();
  Take(';');
  SkipWhitespace();
  CheckEof();

  return serializer;
}

SerializerForDrop SqlParser::ParseDrop() {
  Expect("OP");
  SkipWhitespace();
  Expect("TABLE");
  SkipWhitespace();

  SerializerForDrop serializer;
  serializer.table_name = TakeWord();
  SkipWhitespace();
  Take(';');
  SkipWhitespace();
  CheckEof();

  return serializer;
}

SerializerForInsert SqlParser::ParseInsert() {
  Expect("NSERT");
  SkipWhitespace();
  Expect("INTO");
  SkipWhitespace();

  SerializerForInsert serializer;
  serializer.table_name = TakeWord();
  std::queue<std::string> columns;

  SkipWhitespace();
  Expect('(');
  while (!Eof() && !Test(')')) {
    SkipWhitespace();
    columns.push(TakeWord());
    SkipWhitespace();
    if (!Test(')')) {
      Expect(',');
    }
  }
  Expect(')');
  SkipWhitespace();

  Expect("VALUES");
  SkipWhitespace();
  Expect('(');
  while (!Eof() && !Test(')')) {
    SkipWhitespace();
    std::string buf;
    while (!Eof() && !Test(",)") && !std::isspace(cur_)) {
      if (Take('\'')) {
        buf = ParseString();
      } else {
        buf += Take();
      }
    }
    SkipWhitespace();
    if (buf.empty()) {
      throw Error("Invalid value");
    }
    if (!Test(')')) {
      Expect(',');
    }
    serializer.data.emplace(columns.front(), buf);
    columns.pop();
  }
  Expect(')');
  SkipWhitespace();

  Take(';');
  SkipWhitespace();
  CheckEof();

  return serializer;
}

SerializerForSelect SqlParser::ParseSelect() {
  Expect("ELECT");
  SkipWhitespace();

  SerializerForSelect serializer;
  if (Take('*')) {
    serializer.all_table = true;
  } else {
    while (!Eof()) {
      std::string buf = TakeWord();
      if (Take('.')) {
        if (serializer.table_name1.empty() || buf == serializer.table_name1) {
          serializer.table_name1 = buf;
          serializer.columns1.push_back(TakeWord());
        } else if (serializer.table_name2.empty() || buf == serializer.table_name2) {
          serializer.table_name2 = buf;
          serializer.columns2.push_back(TakeWord());
        } else {
          throw Error("Invalid query");
        }
      } else {
        serializer.unique_columns.push_back(buf);
      }
      SkipWhitespace();
      if (Take(',')) {
        SkipWhitespace();
        continue;
      } else {
        break;
      }
    }
  }

  SkipWhitespace();
  Expect("FROM");
  SkipWhitespace();

  serializer.table_name1 = TakeWord();
  SkipWhitespace();

  if (Take('W')) {
    Expect("HERE");
    SkipWhitespace();
    serializer.filters = ParseFilters();
  } else if (Take('L')) {
    Expect("EFT");
    serializer.join_type = kLeft;
  } else if (Take('R')) {
    Expect("IGHT");
    serializer.join_type = kRight;
  }

  SkipWhitespace();
  ParseJoin(serializer);

  Take(';');
  SkipWhitespace();
  CheckEof();

  return serializer;
}

void SqlParser::ParseJoin(SerializerForSelect& serializer) {
  if (!Take('J')) {
    return;
  }
  Expect("OIN");
  serializer.is_join = true;
  SkipWhitespace();
  std::string buf = TakeWord();
  if (serializer.table_name2.empty()) {
    serializer.table_name2 = buf;
  } else if (serializer.table_name2 != buf) {
    throw Error("Invalid query");
  }
  SkipWhitespace();
  Expect("ON");
  SkipWhitespace();
  buf = TakeWord();
  if (Take('.')) {
    if (buf == serializer.table_name1) {
      serializer.join_columns.first = TakeWord();
    } else if (buf == serializer.table_name2) {
      serializer.join_columns.second = TakeWord();
    } else {
      throw Error("Invalid query");
    }
  } else {
    serializer.join_columns.first = TakeWord();
  }
  SkipWhitespace();
  Expect('=');
  SkipWhitespace();
  buf = TakeWord();
  if (Take('.')) {
    if (buf == serializer.table_name1 && serializer.join_columns.first.empty()) {
      serializer.join_columns.first = TakeWord();
    } else if (buf == serializer.table_name2 && serializer.join_columns.second.empty()) {
      serializer.join_columns.second = TakeWord();
    } else {
      throw Error("Invalid query");
    }
  } else {
    serializer.join_columns.second = TakeWord();
  }
}

SerializerForUpdate SqlParser::ParseUpdate() {
  Expect("PDATE");
  SkipWhitespace();

  SerializerForUpdate serializer;
  serializer.table_name = TakeWord();
  SkipWhitespace();

  Expect("SET");
  SkipWhitespace();

  while (!Eof()) {
    std::string column = TakeWord();
    SkipWhitespace();
    Expect('=');
    SkipWhitespace();
    std::string value = TakeWord();
    SkipWhitespace();

    if (value.empty()) {
      throw Error("Invalid value");
    }

    serializer.values.emplace(column, value);
    if (Take(',')) {
      SkipWhitespace();
      continue;
    } else {
      break;
    }
  }

  if (Take('W')) {
    Expect("HERE");
    SkipWhitespace();
    serializer.filters = ParseFilters();
  }

  Take(';');
  SkipWhitespace();
  CheckEof();

  return serializer;
}

SerializerForDelete SqlParser::ParseDelete() {
  Expect("LETE");
  SkipWhitespace();
  Expect("FROM");
  SkipWhitespace();

  SerializerForDelete serializer;
  serializer.table_name = TakeWord();
  SkipWhitespace();

  if (Take('W')) {
    Expect("HERE");
    SkipWhitespace();
    serializer.filters = ParseFilters();
    serializer.all_table = false;
  }

  Take(';');
  SkipWhitespace();
  CheckEof();

  return serializer;
}


