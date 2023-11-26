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
        SSTableTesting/MemcacheTesting.cpp
        LogDatabaseTesting/LogDatabaseTest.cpp)

target_include_directories(csv INTERFACE lib/csv)

set (LOG_DATABASE_FILES
        LogDatabase/LogDatabaseGenerics.hpp
        LogDatabase/LogDatabase.h
        LogDatabase/LogDatabase.cpp)

set (SSTABLE_FILES
        SSTable/MemCache.h
        SSTable/SSTableDb.h
        SSTable/SSFileCreator.cpp
        SSTable/SSFileCreator.h
        SSTable/SSTableDb.cpp
        SSTable/AvlTree.hpp
        SSTable/SSFile.cpp
        SSTable/SSFile.h
        SSTable/DbMemCache.h)

set (SHARED_FILES
        Workload.h
        Workload.cpp
        DatabaseEntry.h
        DatabaseEntry.cpp
)

add_executable(Databases
        ${LOG_DATABASE_FILES}
        ${SSTABLE_FILES}
        ${SHARED_FILES}
        main.cpp
        SSTable/DbMemCache.h)

target_link_libraries(Databases
        PRIVATE
        fmt::fmt-header-only
        csv)

add_executable(scratchwork test.cpp)

enable_testing()

add_executable(
        SSTableTesting
        ${SSTABLE_FILES}
        ${SHARED_FILES}
        SSTableTesting/SSFileTest.cpp
        SSTableTesting/MemcacheTesting.cpp
        SSTableTesting/TestUtils.cpp
        SSTableTesting/TestUtils.h
)

add_executable(
        LogDatabaseTesting
        ${LOG_DATABASE_FILES}
        ${SHARED_FILES}
        LogDatabaseTesting/LogDatabaseTest.cpp
)


target_link_libraries(
        SSTableTesting
        PRIVATE
        fmt::fmt-header-only
        GTest::gtest_main
        csv
)

target_link_libraries(
        LogDatabaseTesting
        PRIVATE
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(SSTableTesting)
gtest_discover_tests(LogDatabaseTesting)
