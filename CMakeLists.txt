cmake_minimum_required(VERSION 3.24)

project(
        Database
        VERSION 1.0
        DESCRIPTION "C++ Years 2022-2023 Lab-work 12"
        LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)

add_subdirectory(bin)
add_subdirectory(lib)

enable_testing()
add_subdirectory(tests)