#include "BloomFilter.h"
#include <openssl/sha.h>
#include <stdexcept>
#include <utility>

BloomFilter::BloomFilter(int numHashes, size_t numBits, const DbMemCache *memCache, const std::set<std::string> &tombstones) : numHashes(numHashes) {
    bitset = std::vector<uint8_t>(numBits / byteSize, 0);
    memCache->traverseSorted([this](const auto& key, const auto& value){
       auto indices = getBitsetIndices(key);
       for (auto index : indices){
           setBit(index, true);
       }
    });

    for (const auto &key : tombstones){
        auto indices = getBitsetIndices(key);
        for (auto index : indices){
            setBit(index, true);
        }
    }
}

BloomFilter::BloomFilter(int numHashes, std::vector<uint8_t> bitset) : numHashes(numHashes), bitset(std::move(bitset)) {}

bool BloomFilter::canContainKey(const std::string &key) const {
    auto indices = getBitsetIndices(key);
    for (auto index : indices){
        if (!testBit(index)){
            return false;
        }
    }

    return true;
}

std::vector<unsigned long long> BloomFilter::getBitsetIndices(const std::string &str) const {
    std::vector<unsigned long long int> indices;
    auto hashChunks = splitHash(sha256(str), numHashes);
    for (auto chunk : hashChunks){
        indices.push_back(chunk % bitset.size());
    }

    return indices;
}

std::vector<uint8_t> BloomFilter::sha256(const std::string &input) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, input.c_str(), input.length());
    SHA256_Final(hash, &sha256);

    std::vector<std::uint8_t> bytes;
    for (const auto &c : hash){
        bytes.push_back(c);
    }

    return bytes;
}

std::vector<unsigned long long> BloomFilter::splitHash(const std::vector<std::uint8_t> &hash, int splits){
    if (splits >= hash.size()){
        throw std::runtime_error("Splits is larger than hash size");
    }

    size_t maxChunkSizeBytes = 7; //If we have 8 bytes, we might overflow unsigned long long
    size_t chunkSize = std::min(hash.size() / splits, maxChunkSizeBytes);

    std::vector<unsigned long long> chunks;
    int currByte = 0;
    while (splits--){
        unsigned long long chunk = 0;
        for (int i = 0; i < chunkSize; i++){
            chunk |= (static_cast<unsigned long long>(hash[currByte]) << (i * 8));
            currByte++;
        }
        chunks.push_back(chunk);
    }

    return chunks;
}

std::vector<std::uint8_t> BloomFilter::getBitset() const {
    return bitset;
}

void BloomFilter::setBit(size_t bit, bool set) {
    if (bit / byteSize >= bitset.size()){
        throw std::runtime_error("Bit too large");
    }

    if (set){
        bitset[bit / byteSize] |= 1 << (bit % 8);
    } else {
        bitset[bit / byteSize] &= ~(1 << (bit % 8));
    }
}

bool BloomFilter::testBit(size_t bit) const {
    int byteSize = sizeof(decltype(bitset)::value_type);
    if (bit / byteSize >= bitset.size()){
        throw std::runtime_error("Bit too large");
    }

    return bitset[bit / byteSize] & (1 << (bit % 8));
}


