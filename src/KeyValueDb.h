#ifndef DATAINTENSIVE_KEYVALUEDB_H
#define DATAINTENSIVE_KEYVALUEDB_H

#include <optional>

template <class K, class V>
class KeyValueDb {
public:
    virtual void insert(const K& key, const V& value) = 0;
    virtual std::optional<V> get(const K& key) = 0;
    virtual void remove(const K& key) = 0;
    virtual ~KeyValueDb() = default;
};

#endif
