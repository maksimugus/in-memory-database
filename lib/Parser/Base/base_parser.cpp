#include "base_parser.h"

BaseParser::BaseParser(const std::string& s) {
  source_ = Source(s);
  Take();
}

char BaseParser::Take() {
  char result = cur_;
  cur_ = source_.HasNext() ? source_.Next() : kEnd;
  return result;
}

bool BaseParser::Take(char expected) {
  if (Test(expected)) {
    Take();
    return true;
  } else {
    return false;
  }
}

void BaseParser::SkipWhitespace() {
  while (std::isspace(cur_)) {
    Take();
  }
}

void BaseParser::Expect(char expected) {
  if (!Take(expected)) {
    throw Error(std::string("Expected '") + expected + std::string("', found ") + ErrorChar());
  }
}

void BaseParser::Expect(const std::string& s) {
  for (const auto& ch : s) {
    Expect(ch);
  }
}

bool BaseParser::Eof() const {
  return cur_ == kEnd;
}

void BaseParser::CheckEof() {
  if (!Eof()) {
    throw Error(std::string("Expected EOF, found ") + ErrorChar());
  }
}

std::string BaseParser::ErrorChar() const {
  return cur_ == kEnd ? "EOF" : std::string("'") + cur_ + std::string("'");
}

bool BaseParser::Test(char expected) const {
  return cur_ == expected || (std::isalpha(cur_) && cur_ == std::tolower(expected));
}

std::logic_error BaseParser::Error(const std::string& msg) {
  return source_.Error(msg);
}

std::string BaseParser::TakeWord() {
  std::string word;
  while (!Eof() && !std::isspace(cur_) && !Test(",.()")) {
    word += Take();
  }
  return word;
}

std::string BaseParser::ParseString() {
  std::string res;
  while (!Eof() && !Test('\'')) {
    res += Take();
  }
  Expect('\'');
  return res;
}

bool BaseParser::Test(const std::string& s) const {
  return std::any_of(s.begin(), s.end(), [this](char ch) { return Test(ch); });
}

std::vector<Token> BaseParser::ParseFilters() {
  std::vector<Token> tokens;
  Tokenize(tokens);
  std::unordered_map<TokenType, int> operation_priority{
      {kOpenPar, 0},
      {kOr, 1},
      {kAnd, 2},
      {kEquals, 3},
      {kNotEquals, 3},
      {kLess, 3},
      {kGreater, 3},
      {kNotGreater, 3},
      {kNotLess, 3}
  };

  std::vector<Token> postfix_expr;
  std::stack<Token> stack; // operators
  for (const auto& token : tokens) {
    switch (token.type) {
      case kVar:
      case kConst:
        postfix_expr.push_back(token);
        break;
      case kOpenPar:
        stack.push(token);
        break;
      case kClosePar:
        while (!stack.empty() && stack.top().type != kOpenPar) {
          postfix_expr.push_back(stack.top());
          stack.pop();
        }
        if (stack.empty()) {
          throw Error("Invalid logic expression");
        }
        stack.pop();
        break;
      default:
        while (!stack.empty() && operation_priority[stack.top().type] >= operation_priority[token.type]) {
          postfix_expr.push_back(stack.top());
          stack.pop();
        }
        stack.push(token);
        break;
    }
  }

  while (!stack.empty()) {
    postfix_expr.push_back(stack.top());
    stack.pop();
  }

  return postfix_expr;
}

void BaseParser::Tokenize(std::vector<Token>& tokens) {
  while (!Eof()) {
    if (Take('(')) {
      tokens.emplace_back(kOpenPar);
    } else if (Take(')')) {
      tokens.emplace_back(kClosePar);
    } else if (Take('=')) {
      tokens.emplace_back(kEquals);
    } else if (Take('<')) {
      if (Take('=')) {
        tokens.emplace_back(kNotGreater);
      } else if (Take('>')) {
        tokens.emplace_back(kNotEquals);
      } else {
        tokens.emplace_back(kLess);
      }
    } else if (Take('>')) {
      if (Take('=')) {
        tokens.emplace_back(kNotLess);
      } else {
        tokens.emplace_back(kGreater);
      }
    } else {
      std::string tmp = TakeWord();
      if (tmp[0] == '\'') {
        if (tmp[tmp.size() - 1] == '\'') {
          tokens.emplace_back(kConst, tmp.substr(1, tmp.size() - 2));
        } else {
          throw Error(std::string("Expected \', found ") + cur_);
        }
      } else if (std::isalpha(tmp[0])) {
        if (tmp == "OR") {
          tokens.emplace_back(kOr);
        } else if (tmp == "AND") {
          tokens.emplace_back(kAnd);
        } else if (tmp == "TRUE" || tmp == "FALSE") {
          tokens.emplace_back(kConst, tmp);
        } else {
          tokens.emplace_back(kVar, tmp);
        }
      } else {
        tokens.emplace_back(kConst, tmp);
      }
    }
    SkipWhitespace();
  }
}