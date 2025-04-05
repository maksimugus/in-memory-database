#include "lib/Database/database.h"

int main() {
  Database db;
  db.Open("DB1");
  std::cout << db.Execute("SELECT * FROM products") << std::endl;
  return 0;
}