#include "BloomFilter.h"
#include <openssl/sha.h>
#include <stdexcept>

/*
 * Each elements key, hash it with the multiple hash functions
 */

BloomFilter::BloomFilter(int numHashes, const DbMemCache *memCache) : numHashes(numHashes) {
    memCache->traverseSorted([this](const auto& key, const auto& value){
       auto indices = getBitsetIndices(key);
       for (auto index : indices){
           bitset.set(index);
       }
    });
}

bool BloomFilter::canContainKey(const std::string &key) {
    auto indices = getBitsetIndices(key);
    for (auto index : indices){
        if (!bitset.test(index)){
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

