#ifndef DATAINTENSIVE_BLOOMFILTER_H
#define DATAINTENSIVE_BLOOMFILTER_H

#include <set>
#include "SSTableParams.h"
#include "DbMemCache.h"

class BloomFilter {
public:
    /*
     * We don't use std::byte since we want to perform bitwise operations. We expose this type since other
     * files will need it to perform calculations when encoding/decoding a filter.
     */
    using ByteType = uint8_t;

    BloomFilter(int numHashes, size_t numBits, const DbMemCache* memCache, const std::set<std::string> &tombstones);
    BloomFilter(int numHashes, std::vector<ByteType> bitset);
    bool canContainKey(const std::string &key) const;
    std::vector<ByteType> getBitset() const;

private:
    int numHashes;
    /*
     * We choose to use a vector instead of an actual std::bitset (or std::byte) since dynamically sized
     * vectors work much better when writing/reading from a text file.
     */
    std::vector<ByteType> bitset;

    std::vector<unsigned long long> getBitsetIndices(const std::string &str) const;
    static std::vector<ByteType> sha256(const std::string& str);
    static std::vector<unsigned long long> splitHash(const std::vector<ByteType> &hash, int splits);
    void setBit(size_t bit, bool set);
    bool testBit(size_t bit) const;
};


#endif
