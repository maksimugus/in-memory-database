add_library(database Database/database.cpp)
add_library(sql_parser Parser/sql_parser.cpp)
add_library(base_parser Parser/Base/base_parser.cpp)
add_library(source Parser/Base/source.cpp)
target_link_libraries(base_parser source)
target_link_libraries(sql_parser base_parser)
target_link_libraries(database sql_parser)