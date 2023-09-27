cmake_minimum_required(VERSION 3.26)
project(DataIntensive)

set(CMAKE_CXX_STANDARD 20)

add_executable(SSTable SSTable/main.cpp
        SSTable/AvlTree.h
        SSTable/SSTableDb.h
        SSTable/DatabaseEntry.h
        SSTable/AvlTree.cpp
        SSTable/SSTableFile.cpp
        SSTable/SSTableFile.h
        SSTable/SSTableDb.cpp)

add_executable(LogDatabase LogDatabase/main.cpp
        LogDatabase/LogDatabase.h)


