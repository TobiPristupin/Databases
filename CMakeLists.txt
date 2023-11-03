cmake_minimum_required(VERSION 3.26)
project(DataIntensive)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

FetchContent_Declare(fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG master
)

FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

FetchContent_MakeAvailable(fmt)
FetchContent_MakeAvailable(googletest)

add_library(csv INTERFACE
        SSTableTesting/MemcacheTesting.cpp)
target_include_directories(csv INTERFACE lib/csv)

add_executable(SSTable SSTable/main.cpp
        SSTable/MemCache.h
        SSTable/SSTableDb.h
        SSTable/DatabaseEntry.h
        SSTable/SSFileCreator.cpp
        SSTable/SSFileCreator.h
        SSTable/SSTableDb.cpp
        SSTable/DatabaseEntry.cpp
        SSTable/AvlTree.hpp
        SSTable/SSFile.cpp
        SSTable/SSFile.h
        SSTable/Workload.h
        SSTable/Workload.cpp)

add_executable(LogDatabase LogDatabase/main.cpp
        LogDatabase/LogDatabase.h)

target_link_libraries(SSTable
        PRIVATE
        fmt::fmt-header-only
        csv)
add_executable(scratchwork test.cpp)

enable_testing()

add_executable(
        SSTableTesting
        SSTableTesting/SSFileTest.cpp
        SSTableTesting/MemcacheTesting.cpp
        SSTable/MemCache.h
        SSTable/SSTableDb.h
        SSTable/DatabaseEntry.h
        SSTable/SSFileCreator.cpp
        SSTable/SSFileCreator.h
        SSTable/SSTableDb.cpp
        SSTable/DatabaseEntry.cpp
        SSTable/AvlTree.hpp
        SSTable/SSFile.cpp
        SSTable/SSFile.h
        SSTableTesting/TestUtils.cpp
        SSTableTesting/TestUtils.h
        SSTable/Workload.h
        SSTable/Workload.cpp
)

target_link_libraries(
        SSTableTesting
        PRIVATE
        fmt::fmt-header-only
        GTest::gtest_main
        csv
)

include(GoogleTest)
gtest_discover_tests(SSTableTesting)
