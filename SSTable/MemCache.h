#ifndef DATAINTENSIVE_MEMCACHE_H
#define DATAINTENSIVE_MEMCACHE_H

#include <string>
#include <optional>
#include <utility>
#include <vector>
#include <memory>
#include <stack>
#include <functional>


template<class K, class V>
class MemCache {
public:
    virtual void insert(const K &key, const V &value) = 0;
    virtual std::optional<V> get(const K &k) const = 0;
    virtual bool remove(const K& key) = 0;
    virtual void traverseSorted(const std::function<void(const K& k, const V& v)>& callback) const = 0;
    virtual size_t size() const = 0;
    virtual void clear() = 0;
    virtual ~MemCache() = default;
};

#endif
