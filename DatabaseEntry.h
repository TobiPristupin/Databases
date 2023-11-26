#ifndef DATAINTENSIVE_DATABASEENTRY_H
#define DATAINTENSIVE_DATABASEENTRY_H

#include <utility>
#include <variant>
#include <string>

using DbValue = std::variant<int, long, double, bool, std::string>;
using DbValueTypeIndex = size_t;
constexpr int intType = 0;
constexpr int longType = 1;
constexpr int doubleType = 2;
constexpr int boolType = 3;
constexpr int stringType = 4;

const size_t maxKeySize = 1024;

std::string dbValueToString(const DbValue& value);
DbValue dbValueFromString(DbValueTypeIndex typeIndex, const std::string &value);
DbValue dbValueFromBytes(DbValueTypeIndex typeIndex, const char* data);

#endif
