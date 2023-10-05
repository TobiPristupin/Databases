cmake_minimum_required(VERSION 3.26)
project(DataIntensive)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG master
)
FetchContent_MakeAvailable(fmt)

add_library(csv INTERFACE)
target_include_directories(csv INTERFACE lib/csv)

add_executable(SSTable SSTable/main.cpp
        SSTable/MemCache.h
        SSTable/SSTableDb.h
        SSTable/DatabaseEntry.h
        SSTable/SSTableFile.cpp
        SSTable/SSTableFile.h
        SSTable/SSTableDb.cpp
        SSTable/DatabaseEntry.cpp
        SSTable/AvlTree.cpp
        SSTable/AvlTree.h)

add_executable(LogDatabase LogDatabase/main.cpp
        LogDatabase/LogDatabase.h)

target_link_libraries(SSTable PRIVATE fmt::fmt-header-only csv)
add_executable(test test.cpp)
