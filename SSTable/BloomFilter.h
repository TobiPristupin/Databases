#ifndef DATAINTENSIVE_BLOOMFILTER_H
#define DATAINTENSIVE_BLOOMFILTER_H

#include <set>
#include "SSTableParams.h"
#include "DbMemCache.h"

class BloomFilter {
public:
    BloomFilter(int numHashes, size_t numBits, const DbMemCache* memCache, const std::set<std::string> &tombstones);
    BloomFilter(int numHashes, std::vector<uint8_t> bitset);
    bool canContainKey(const std::string &key) const;
    std::vector<std::uint8_t> getBitset() const;

private:
    int numHashes;
    /*
     * We choose to use a vector instead of an actual std::bitset (or std::byte) since dynamically sized
     * vectors work much better when writing/reading from a text file.
     */
    std::vector<uint8_t> bitset;
    size_t byteSize = sizeof(uint8_t);

    std::vector<unsigned long long> getBitsetIndices(const std::string &str) const;
    static std::vector<uint8_t> sha256(const std::string& str);
    static std::vector<unsigned long long> splitHash(const std::vector<std::uint8_t> &hash, int splits);
    void setBit(size_t bit, bool set);
    bool testBit(size_t bit) const;
};


#endif
