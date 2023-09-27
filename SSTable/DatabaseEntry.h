#ifndef DATAINTENSIVE_DATABASEENTRY_H
#define DATAINTENSIVE_DATABASEENTRY_H

#include <utility>
#include <variant>
#include <string>

using DbValue = std::variant<int, long, double, bool, std::string>;

struct Entry {
    Entry(std::string key, DbValue value) : key(std::move(key)), value(std::move(value)) {}
    std::string key;
    DbValue value;
};

#endif
