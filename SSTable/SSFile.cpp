#include "SSFile.h"
#include <utility>
#include <iostream>

SSFile::SSFile(std::fstream file, size_t index, offset keyFooterStart)
        : file(std::move(file)), index(index), keyFooterStart(keyFooterStart) {}

size_t SSFile::getIndex() const {
    return index;
}

std::optional<DbValue> SSFile::get(const std::string &key) {
    auto chunkHeader = moveToChunkForKey(key);
    offset chunkStart = file.tellg();
    auto valueOffset = findValueOffset(chunkStart, chunkHeader, key);
    if (!valueOffset.has_value()){
        return std::nullopt;
    }

    file.seekg(valueOffset.value());
    return readValue();
}

std::optional<SSFile::offset> SSFile::findValueOffset(SSFile::offset chunkStart, SSFile::KeyChunkHeader chunkHeader, const std::string &key) {
    auto lo = 0;
    auto hi = chunkHeader.getNumKeysInChunk() - 1;
    auto mid = lo + ((hi - lo) / 2);
    while (lo <= hi){
        file.seekg(chunkStart + mid * chunkHeader.keyOffsetPairLength());
        auto keyOffsetPair = readKeyOffsetPair(chunkHeader.getFixedKeySize());
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
    file.seekg(keyFooterStart);
    auto chunkHeader = readKeyChunkHeader();
    /*
     * We are guaranteed to find a corresponding chunk, since we limit the max size of a key, and we make a
     * chunk that can fit keys up to the max size.
    */
    while (key.size() > chunkHeader.getFixedKeySize()){
        file.seekg(chunkHeader.getLength(), std::ios::cur);
        chunkHeader = readKeyChunkHeader();
    }

    return chunkHeader;
}

SSFile::KeyChunkHeader SSFile::readKeyChunkHeader() {
    KeyChunkHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    return header;
}

SSFile::KeyOffsetPair SSFile::readKeyOffsetPair(size_t fixedKeySize) {
    std::vector<char> key(fixedKeySize, 0);
    file.read(key.data(), fixedKeySize);
    auto str = std::string(key.begin(), key.end());
    str.resize(str.find_first_of('\00'));
    offset pos;
    file.read(reinterpret_cast<char*>(&pos), sizeof(offset));
    return {str, pos};
}

SSFile::ValueHeader SSFile::readValueHeader() {
    ValueHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    return header;
}

DbValue SSFile::readValue() {
    auto header = readValueHeader();
    std::vector<char> data(header.dataLength, 0);
    file.read(data.data(), header.dataLength);
    return dbValueFromString(header.typeIndex, std::string(data.begin(), data.end()));
}

SSFile::ValueHeader::ValueHeader(uint32_t dataLength, uint32_t typeIndex) : dataLength(dataLength), typeIndex(typeIndex) {}

SSFile::ValueHeader::ValueHeader() : dataLength(0), typeIndex(0) {}

bool SSFile::ValueHeader::isEntryRemoved() const {
    return dataLength == 0;
}

SSFile::KeyChunkHeader::KeyChunkHeader(uint32_t fixedKeySize, uint32_t chunkLength)
        : fixedKeySize(fixedKeySize), length(chunkLength) {
    if (chunkLength % (fixedKeySize + sizeof(offset)) != 0){
        throw std::runtime_error("Key chunk malformed. Expected the length of the chunk to be a multiple of the size of a key-offset pair."
                                 " Size of key " + std::to_string(fixedKeySize) + ", length of chunk " + std::to_string(chunkLength));
    }
}

SSFile::KeyChunkHeader::KeyChunkHeader() : fixedKeySize(0), length(0) {}

size_t SSFile::KeyChunkHeader::keyOffsetPairLength() const {
    return fixedKeySize + sizeof(offset);
}

uint32_t SSFile::KeyChunkHeader::getFixedKeySize() const {
    return fixedKeySize;
}

uint32_t SSFile::KeyChunkHeader::getLength() const {
    return length;
}

size_t SSFile::KeyChunkHeader::getNumKeysInChunk() const {
    return length / keyOffsetPairLength();
}

SSFile::KeyOffsetPair::KeyOffsetPair(std::string key, SSFile::offset pos) : key(std::move(key)), pos(pos) {}
