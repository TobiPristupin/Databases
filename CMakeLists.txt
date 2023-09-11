cmake_minimum_required(VERSION 3.26)
project(DataIntensive)

set(CMAKE_CXX_STANDARD 17)

add_executable(LogDatabase LogDatabase/main.cpp
        LogDatabase/LogDatabase.h)
