#ifndef DATAINTENSIVE_MEMCACHE_H
#define DATAINTENSIVE_MEMCACHE_H

#include <string>
#include <optional>
#include <utility>
#include <vector>
#include <memory>
#include <stack>
#include "DatabaseEntry.h"

class MemCache {
public:
    virtual void insert(const Entry &entry) = 0;
    virtual std::optional<DbValue> get(const std::string &key) const = 0;
    virtual bool remove(const std::string& key) = 0;
    virtual void traverseSorted(const std::function<void(const Entry&)>& callback) const = 0;
    virtual size_t size() const = 0;
    virtual void clear() = 0;
};

#endif
