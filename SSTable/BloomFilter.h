#ifndef DATAINTENSIVE_BLOOMFILTER_H
#define DATAINTENSIVE_BLOOMFILTER_H

#include <bitset>
#include "SSTableParams.h"
#include "DbMemCache.h"

class BloomFilter {

public:
    BloomFilter(int numHashes, const DbMemCache* memCache);
    bool canContainKey(const std::string &key);

private:
    int numHashes;
    std::bitset<bloomFilterBits> bitset;

    std::vector<unsigned long long> getBitsetIndices(const std::string &str) const;
    static std::vector<uint8_t> sha256(const std::string& str);
    static std::vector<unsigned long long> splitHash(const std::vector<std::uint8_t> &hash, int splits);
};


#endif
