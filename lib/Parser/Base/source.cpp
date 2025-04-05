#include "source.h"

Source::Source(const std::string& str) : str(str) {}

bool Source::HasNext() const {
  return pos < str.length();
}

char Source::Next() {
  return str[pos++];
}

std::logic_error Source::Error(const std::string& msg) const {
  return std::logic_error(std::to_string(pos) + ": " + msg);
}