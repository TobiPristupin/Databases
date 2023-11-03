#ifndef DATAINTENSIVE_DATABASEENTRY_H
#define DATAINTENSIVE_DATABASEENTRY_H

#include <utility>
#include <variant>
#include <string>
#include "MemCache.h"

using DbValue = std::variant<int, long, double, bool, std::string>;
constexpr int intType = 0;
constexpr int longType = 1;
constexpr int doubleType = 2;
constexpr int boolType = 3;
constexpr int stringType = 4;


using DbMemCache = MemCache<std::string, DbValue>;

const size_t maxKeySize = 1024;

std::string dbValueToString(const DbValue& value);
DbValue dbValueFromString(size_t typeIndex, const std::string &value);
DbValue dbValueFromBytes(int typeIndex, const char* data);

#endif
