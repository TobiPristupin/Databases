#ifndef DATAINTENSIVE_DATABASEENTRY_H
#define DATAINTENSIVE_DATABASEENTRY_H

#include <utility>
#include <variant>
#include <string>

using DbValue = std::variant<int, long, double, bool, std::string>;

struct Entry {
    Entry(std::string key, int type_index, const std::string &value);
    Entry(std::string key, DbValue value);
    std::string key;
    DbValue value;

    std::string valueToString() const;

};

#endif
