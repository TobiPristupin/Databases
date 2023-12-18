#include "SSFile.h"
#include <utility>
#include <iostream>

SSFile::SSFile(std::fstream file) : file(std::move(file)) {
    header = readSSFileHeader();
    if (header.hasBloomFilter()){
        bloomFilter = readBloomFilter(header.bloomFilterLength());
    }
}

SSFileRead SSFile::get(const std::string &key) {
    if (bloomFilter.has_value()){
        if (!bloomFilter.value().canContainKey(key)){
            return {KEY_NOT_FOUND};
        }
    }

    auto chunkHeader = moveToChunkForKey(key);
    offset chunkStart = file.tellg();
    auto valueOffset = findValueOffset(chunkStart, chunkHeader, key);
    if (!valueOffset.has_value()){
        return {KEY_NOT_FOUND};
    }

    file.seekg(valueOffset.value());
    auto valueHeader = readValueHeader();
    if (valueHeader.isEntryRemoved()){
        return {KEY_TOMBSTONE};
    }

    auto value =  readValue(valueHeader);
    return {KEY_FOUND, value};
}

size_t SSFile::getIndex() const {
    return header.index;
}

std::optional<SSFile::offset> SSFile::findValueOffset(SSFile::offset chunkStart, SSFile::KeyChunkHeader chunkHeader, const std::string &key) {
    int lo = 0;
    int hi = static_cast<int>(chunkHeader.getNumKeysInChunk() - 1);
    int mid = lo + ((hi - lo) / 2);
    while (lo <= hi){
        file.seekg(chunkStart + mid * chunkHeader.keyOffsetPairLength());
        auto keyOffsetPair = readKeyOffsetPair(chunkHeader.fixedKeySize);
        if (key == keyOffsetPair.key){
            return keyOffsetPair.pos;
        } else if (key < keyOffsetPair.key){
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
        mid = lo + ((hi - lo) / 2);
    }

    return std::nullopt;
}

SSFile::KeyChunkHeader SSFile::moveToChunkForKey(const std::string &key) {
    file.seekg(header.keyFooterStart);
    auto chunkHeader = readKeyChunkHeader();
    /*
     * We are guaranteed to find a corresponding chunk, since we limit the max size of a key, and we make a
     * chunk that can fit keys up to the max size.
    */
    while (key.size() > chunkHeader.fixedKeySize){
        file.seekg(chunkHeader.length, std::ios::cur);
        chunkHeader = readKeyChunkHeader();
    }

    return chunkHeader;
}

SSFile::SSFileHeader SSFile::readSSFileHeader() {
    SSFileHeader ssFileHeader{};
    file.read(reinterpret_cast<char*>(&ssFileHeader), sizeof(ssFileHeader));
    return ssFileHeader;
}

BloomFilter SSFile::readBloomFilter(uint32_t bloomFilterLength) {
    std::vector<uint8_t> bitset(bloomFilterLength);
    file.read(reinterpret_cast<char*>(bitset.data()), bloomFilterLength);
    return {SSTable::bloomFilterHashes, bitset};
}

SSFile::KeyChunkHeader SSFile::readKeyChunkHeader() {
    KeyChunkHeader keyChunkHeader{};
    file.read(reinterpret_cast<char*>(&keyChunkHeader), sizeof(keyChunkHeader));
    return keyChunkHeader;
}

SSFile::KeyOffsetPair SSFile::readKeyOffsetPair(size_t fixedKeySize) {
    std::vector<char> key(fixedKeySize, 0);
    file.read(key.data(), fixedKeySize);
    auto str = std::string(key.begin(), key.end());
    auto paddingStart = str.find_first_of('\00');
    if (paddingStart != std::string::npos){
        str.resize(paddingStart);
    }

    offset pos;
    file.read(reinterpret_cast<char*>(&pos), sizeof(offset));
    return {str, pos};
}

SSFile::ValueHeader SSFile::readValueHeader() {
    ValueHeader valueHeader{};
    file.read(reinterpret_cast<char*>(&valueHeader), sizeof(valueHeader));
    return valueHeader;
}

DbValue SSFile::readValue(const ValueHeader &valueHeader) {
    std::vector<char> data(valueHeader.dataLength, 0);
    file.read(data.data(), valueHeader.dataLength);
    return dbValueFromString(valueHeader.typeIndex, std::string(data.begin(), data.end()));
}

SSFile::ValueHeader::ValueHeader(uint32_t dataLength, DbValueTypeIndex typeIndex) : dataLength(dataLength), typeIndex(typeIndex) {}

bool SSFile::ValueHeader::isEntryRemoved() const {
    return dataLength == 0;
}
SSFile::ValueHeader SSFile::ValueHeader::TombstoneHeader() {
    return {0, 0};
}

SSFile::KeyChunkHeader::KeyChunkHeader(uint32_t fixedKeySize, uint32_t chunkLength)
        : fixedKeySize(fixedKeySize), length(chunkLength) {
    if (chunkLength % (fixedKeySize + sizeof(offset)) != 0){
        throw std::runtime_error("Key chunk malformed. Expected the length of the chunk to be a multiple of the size of a key-offset pair."
                                 " Size of key " + std::to_string(fixedKeySize) + ", length of chunk " + std::to_string(chunkLength));
    }
}

size_t SSFile::KeyChunkHeader::keyOffsetPairLength() const {
    return fixedKeySize + sizeof(offset);
}

size_t SSFile::KeyChunkHeader::getNumKeysInChunk() const {
    if (length % keyOffsetPairLength() != 0){
        throw std::runtime_error("Chunk malformed");
    }
    return length / keyOffsetPairLength();
}

SSFile::KeyOffsetPair::KeyOffsetPair(std::string key, SSFile::offset pos) : key(std::move(key)), pos(pos) {}


SSFile::SSFileHeader::SSFileHeader(uint32_t index, uint32_t bloomFilterLength,
                                   uint32_t footerStart) : index(index),
                                                           filterBits(bloomFilterLength),
                                                           keyFooterStart(footerStart) {}

bool SSFile::SSFileHeader::hasBloomFilter() const {
    return filterBits > 0;
}

size_t SSFile::SSFileHeader::bloomFilterLength() const {
    return filterBits / sizeof(BloomFilter::ByteType);
}
