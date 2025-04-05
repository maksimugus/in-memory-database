#pragma once

#include <stack>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "source.h"

const char kEnd = 0;

enum TokenType {
  kVar,
  kConst,
  kRes,
  kEquals,
  kNotEquals,
  kGreater,
  kLess,
  kNotGreater,
  kNotLess,
  kOr,
  kAnd,
  kOpenPar,
  kClosePar
};

struct Token {
  TokenType type;
  std::string value;
};

class BaseParser {
 public:
  explicit BaseParser(const std::string& s);

 protected:
  Source source_;
  char cur_;

  /// вернуть текущий символ и перейти к следующему
  char Take();

  /// Take с проверкой
  bool Take(char expected);

  std::string TakeWord();

  /// пропустить служебные символы
  void SkipWhitespace();

  /// проверить, что текущий символ соответствует ожидаемому
  void Expect(char expected);

  void Expect(const std::string& s);

  /// проверить на конец
  void CheckEof();

  bool Eof() const;

  std::string ErrorChar() const;

  bool Test(char expected) const;

  bool Test(const std::string& s) const;

  std::logic_error Error(const std::string& msg);

  std::string ParseString();

  std::vector<Token> ParseFilters();

 private:
  void Tokenize(std::vector<Token>& tokens);
};
