#pragma once

#include <stdexcept>

class Source {
 public:
  Source() = default;

  explicit Source(const std::string& str);

  /// проверка на наличие следующего символа
  bool HasNext() const;

/// получение следующего символа
  char Next();

  /// ошибка для некорректного ввода
  std::logic_error Error(const std::string& msg) const;

 private:
  std::string str;
  size_t pos = 0;
};