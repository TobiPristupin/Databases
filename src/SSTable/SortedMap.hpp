#ifndef DATAINTENSIVE_SORTEDMAP_HPP
#define DATAINTENSIVE_SORTEDMAP_HPP

#include <functional>
#include <map>
#include "MemCache.h"

template<class K, class V>
class SortedMap : public MemCache<K, V> {
public:

    SortedMap() = default;
    std::optional<V> get(const K& k) const override;
    void insert(const K& k, const V& v) override;
    bool remove(const K& key) override;
    void traverseSorted(const std::function<void(const K& k, const V& v)>& callback) const override;
    [[nodiscard]] size_t size() const override;
    void clear() override;

private:
    std::map<K, V> map;
};

template<class K, class V>
void SortedMap<K, V>::clear() {
    map.clear();
}

template<class K, class V>
size_t SortedMap<K, V>::size() const {
    return map.size();
}

template<class K, class V>
void SortedMap<K, V>::traverseSorted(const std::function<void(const K &, const V &)> &callback) const {
    for (const auto &pair: map){
        callback(pair.first, pair.second);
    }
}

template<class K, class V>
bool SortedMap<K, V>::remove(const K &key) {
    return map.erase(key) > 1;
}

template<class K, class V>
void SortedMap<K, V>::insert(const K &k, const V &v) {
    map.insert({k, v});
}

template<class K, class V>
std::optional<V> SortedMap<K, V>::get(const K &k) const {
    if (map.find(k) != map.end()){
        return map.at(k);
    }

    return std::nullopt;
}


#endif
