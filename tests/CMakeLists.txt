include(FetchContent)

FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.13.0
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(database_tests database_tests.cpp)

target_include_directories(database_tests PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(database_tests database GTest::gtest_main)

include(GoogleTest)

gtest_discover_tests(database_tests)