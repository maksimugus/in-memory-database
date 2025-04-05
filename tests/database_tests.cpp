#include <gtest/gtest.h>

#include "lib/Database/database.h"

TEST(DatabaseTests, ValidCreateTableTest1) {
  Database db;
  std::cout << db.Execute(
      R"(
              CREATE TABLE student (
              student_id INT PRIMARY KEY,
              name VARCHAR(20),
              major VARCHAR(20))
              )"
  ) << std::endl;
}

TEST(DatabaseTests, ValidCreateTableTest2) {
  Database db;
  std::cout << db.Execute(
      R"(
              create table products (
              product_id int primary key,
              name varchar(10),
              price double,
              weight float)
              )"
  ) << std::endl;
}

///primary key is not set
TEST(DatabaseTests, InvalidCreateTableTest1) {
  Database db;
  try {
    db.Execute(
        R"(
              CREATE TABLE student (
              student_id INT,
              name VARCHAR(20),
              major VARCHAR(20))
              )"
    );
  } catch (const std::logic_error& e) {
    std::cout << e.what() << std::endl;
  }
}

///varchar var_size is not set
TEST(DatabaseTests, InvalidCreateTableTest2) {
  Database db;
  try {
    db.Execute(
        R"(
              create table products (
              product_id int primary key,
              name varchar(),
              price double,
              weight float)
              )"
    );
  } catch (const std::logic_error& e) {
    std::cout << e.what() << std::endl;
  }
}

TEST(DatabaseTests, InsertTest) {
  Database db;
  db.Execute(
      R"(
              create table products (
              product_id int primary key,
              name varchar(20),
              price double,
              weight float)
              )"
  );
  std::cout << db.Execute("INSERT INTO products(product_id, price) VALUES(239, 23.9)") << std::endl;
  db.Save("DB1");
}

TEST(DatabaseTests, SelectTest) {
  Database db;
  db.Execute(
      R"(
              CREATE TABLE employee (
                emp_id INT PRIMARY KEY,
                first_name VARCHAR(20),
                last_name VARCHAR(20),
                sex VARCHAR(1),
                salary INT,
                super_id INT)
              )"
  );
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(184, John, Corner, M, 100000, NULL))");
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(348, Boba, Fat, M, 80000, 184))");
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(235, Ray, Skytalker, F, 60000, 348))");
  std::cout << db.Execute("SELECT * FROM employee") << std::endl;
  std::cout << db.Execute(R"(SELECT first_name, last_name FROM employee
                                    WHERE salary <> 80000 AND sex = 'M' OR sex = 'F')") << std::endl;
}

TEST(DatabaseTests, DropTableTest) {
  Database db;
  db.Execute(
      R"(
              CREATE TABLE employee (
                emp_id INT PRIMARY KEY,
                first_name VARCHAR(20),
                last_name VARCHAR(20),
                sex VARCHAR(1),
                salary INT,
                super_id INT)
              )"
  );
  std::cout << db.Execute("DROP TABLE employee") << std::endl;
}

TEST(DatabaseTests, UpdateTest) {
  Database db;
  db.Execute(
      R"(
              CREATE TABLE employee (
                emp_id INT PRIMARY KEY,
                first_name VARCHAR(20),
                last_name VARCHAR(20),
                sex VARCHAR(1),
                salary INT,
                super_id INT)
              )"
  );
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(184, John, Corner, M, 100000, NULL))");
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(348, Boba, Fat, M, 80000, 184))");
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(235, Ray, Skytalker, F, 60000, 348))");
  std::cout << db.Execute(R"(
                UPDATE employee
                SET super_id = NULL
                WHERE sex = 'F'
  )") << std::endl;
  std::cout << db.Execute(R"(
                UPDATE employee
                SET salary = 100500
                WHERE first_name = 'Ray' AND last_name = 'Skytalker' OR last_name = 'Corner' AND sex = 'M'
  )") << std::endl;
  std::cout << db.Execute("SELECT * FROM employee") << std::endl;
}

TEST(DatabaseTests, DeleteTest) {
  Database db;
  db.Execute(R"(
    CREATE TABLE employee (
      emp_id INT PRIMARY KEY,
      first_name VARCHAR(20),
      last_name VARCHAR(20),
      sex VARCHAR(1),
      salary INT,
      super_id INT
    )
  )");
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(184, John, Corner, M, 100000, NULL))");
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(348, Boba, Fat, M, 80000, 184))");
  db.Execute(R"(INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
             VALUES(235, Ray, Skytalker, F, 60000, 348))");
//  std::cout << db.Execute("DELETE FROM employee") << std::endl;
  std::cout << db.Execute("DELETE FROM employee WHERE sex = 'M'") << std::endl;
  std::cout << db.Execute("SELECT * FROM employee") << std::endl;
}

TEST(DatabaseTests, JoinTest) {
  Database db;
  db.Execute(R"(
    CREATE TABLE employee (
      emp_id INT PRIMARY KEY,
      first_name VARCHAR(40),
      last_name VARCHAR(40),
      sex VARCHAR(1),
      salary INT,
      super_id INT,
      branch_id INT
    )
  )");
  db.Execute(R"(
    CREATE TABLE branch (
      branch_id INT PRIMARY KEY,
      branch_name VARCHAR(40),
      mgr_id INT
    )
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary)
    VALUES(100, 'David', 'Wallace', 'M', 250000)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id, branch_id)
    VALUES(101, 'Jan', 'Levinson', 'F', 110000, 100, 1)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
    VALUES(102, 'Michael', 'Scott', 'M', 75000, 100)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id, branch_id)
    VALUES(103, 'Angela', 'Martin', 'F', 63000, 102, 2)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id, branch_id)
    VALUES(104, 'Kelly', 'Kapoor', 'F', 55000, 102, 2)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id, branch_id)
    VALUES(105, 'Stanley', 'Hudson', 'M', 69000, 102, 2)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id)
    VALUES(106, 'Josh', 'Porter', 'M', 78000, 100)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id, branch_id)
    VALUES(107, 'Andy', 'Bernard', 'M', 65000, 106, 3)
  )");
  db.Execute(R"(
    INSERT INTO employee(emp_id, first_name, last_name, sex, salary, super_id, branch_id)
    VALUES(108, 'Jim', 'Halpert', 'M', 71000, 106, 3)
  )");
  db.Execute(R"(INSERT INTO branch(branch_id, branch_name, mgr_id)
                      VALUES(1, 'Corporate', 100))");
  db.Execute(R"(INSERT INTO branch(branch_id, branch_name, mgr_id)
                      VALUES(2, 'Scranton', 102))");
  db.Execute(R"(INSERT INTO branch(branch_id, branch_name, mgr_id)
                      VALUES(3, 'Stamford', 106))");
  db.Execute(R"(INSERT INTO branch(branch_id, branch_name, mgr_id)
                      VALUES(4, 'Houston', 110))");
  db.Save("COMPANY");
  db.Open("COMPANY");
  std::cout << db.Execute(R"(
                SELECT employee.emp_id, employee.first_name, branch.branch_name
                FROM employee
                JOIN branch
                ON employee.emp_id = branch.mgr_id
  )") << std::endl;
}