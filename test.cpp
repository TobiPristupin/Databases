#include <memory>
#include <iostream>
#include <cstring>
#include <fstream>
#include <regex>
#include <variant>
#include <random>
#include <openssl/sha.h>

std::vector<uint8_t> sha256(const std::string &input) {
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

std::vector<unsigned long long> splitHash(const std::vector<std::uint8_t> &hash, int splits){
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

int main() {
    std::cout << (std::string("v") > std::string("O")) << "\n";
}