cmake_minimum_required(VERSION 3.26)
project(DataIntensive)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
add_compile_options(-Wno-dev)

FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG master
)

FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

FetchContent_Declare(
        googlebenchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG main)


FetchContent_MakeAvailable(fmt)
FetchContent_MakeAvailable(googletest)
FetchContent_MakeAvailable(googlebenchmark)
find_package(OpenSSL REQUIRED)


add_library(csv INTERFACE
        src/SSTableTesting/MemcacheTesting.cpp
        src/LogDatabaseTesting/LogDatabaseTest.cpp)

target_include_directories(csv INTERFACE lib/csv)

set (LOG_DATABASE_FILES
        src/LogDatabase/LogDatabaseGenerics.hpp
        src/LogDatabase/LogDatabase.h
        src/LogDatabase/LogDatabase.cpp)

set (SSTABLE_FILES
        src/SSTable/MemCache.h
        src/SSTable/SSTableDb.h
        src/SSTable/SSFileCreator.cpp
        src/SSTable/SSFileCreator.h
        src/SSTable/SSTableDb.cpp
        src/SSTable/BST.hpp
        src/SSTable/SSFile.cpp
        src/SSTable/SSFile.h
        src/SSTable/DbMemCache.h
        src/SSTable/SortedMap.hpp
        src/SSTable/SSTableParams.h
        src/SSTable/BloomFilter.h
        src/SSTable/BloomFilter.cpp)

set (SHARED_FILES
        src/Workload.h
        src/Workload.cpp
        src/DatabaseEntry.h
        src/DatabaseEntry.cpp
        src/KeyValueDb.h
)

add_executable(Databases
        ${LOG_DATABASE_FILES}
        ${SSTABLE_FILES}
        ${SHARED_FILES}
        src/main.cpp
        src/SSTable/DbMemCache.h)

target_link_libraries(Databases
        PRIVATE
        fmt::fmt-header-only
        csv
        benchmark::benchmark
        OpenSSL::SSL)

add_executable(scratchwork test.cpp)

enable_testing()

add_executable(
        SSTableTesting
        ${SSTABLE_FILES}
        ${SHARED_FILES}
        src/SSTableTesting/SSFileTest.cpp
        src/SSTableTesting/MemcacheTesting.cpp
        src/SSTableTesting/TestUtils.cpp
        src/SSTableTesting/TestUtils.h
        src/SSTableTesting/BloomFilterTesting.cpp
        src/SSTableTesting/SSTableTesting.cpp
)

add_executable(
        LogDatabaseTesting
        ${LOG_DATABASE_FILES}
        ${SHARED_FILES}
        src/LogDatabaseTesting/LogDatabaseTest.cpp
)


target_link_libraries(
        SSTableTesting
        PRIVATE
        fmt::fmt-header-only
        GTest::gtest_main
        csv
        OpenSSL::SSL
)

target_link_libraries(
        LogDatabaseTesting
        PRIVATE
        GTest::gtest_main
)

target_link_libraries(
        scratchwork
        PRIVATE
        OpenSSL::SSL
)

include(GoogleTest)
gtest_discover_tests(SSTableTesting)
gtest_discover_tests(LogDatabaseTesting)
