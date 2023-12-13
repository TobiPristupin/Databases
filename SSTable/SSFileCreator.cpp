#include <unordered_set>
#include <iostream>
#include "SSFileCreator.h"
#include "fmt/format.h"


std::unique_ptr<SSFile> SSFileCreator::newFile(const std::filesystem::path &directory, size_t index,
                                               bool hasBloomFilter, uint32_t bloomFilterSize,
                                               const DbMemCache *memcache,  const std::unordered_set<std::string> &tombstones) {
    auto filename = fmt::format(fmt::runtime(ssTableFilenameFormat), index);
    std::fstream stream;
    stream.exceptions(std::ios::badbit | std::ios::failbit);
    auto dir = directory / filename;
    stream.open(directory / filename, std::ios::out | std::ios::in | std::ios::trunc | std::ios::binary);
    auto headerStart = writePlaceHolderSSFileHeader(&stream);
    auto footerStart = writeToFile(&stream, memcache, tombstones);
    modifySSFileHeader(&stream, headerStart, SSFileHeader(index, hasBloomFilter, bloomFilterSize, footerStart));
    stream.seekg(0);
    return std::make_unique<SSFile>(std::move(stream));
}

std::unique_ptr<SSFile> SSFileCreator::loadFile(const std::filesystem::path &file) {
    //TODO: FIX
    if (!isFilenameSSTable(file)){
        throw std::runtime_error("File " + file.string() + " is not a valid SSTable file");
    }

    std::fstream stream;
    stream.open(file, std::ios::in | std::ios::binary);
    return std::make_unique<SSFile>(std::move(stream));
}

SSFileCreator::offset SSFileCreator::writeToFile(std::fstream *stream, const DbMemCache *memcache,
                                                 const std::unordered_set<std::string> &tombstones) {
    auto valueOffsets = writeValues(stream, memcache, tombstones);
    auto keysBySize = groupByChunkKeySize(memcache);
    offset footerStart = writeKeyChunks(stream, keysBySize, valueOffsets);
    return footerStart;
}

std::map<std::string, SSFileCreator::offset> SSFileCreator::writeValues(std::fstream *stream, const DbMemCache *memcache,
                                                                        const std::unordered_set<std::string> &tombstones) {
    std::map<std::string, offset> offsets;
    memcache->traverseSorted([&](const std::string &key, const DbValue& value){
        ValueHeader header;
        if (tombstones.find(key) != tombstones.end()){
            header = ValueHeader(0, 0);
        } else {
            header = ValueHeader(dbValueToString(value).size(), value.index());
        }

        offsets[key] = writeValue(stream, header, value);
    });
    return offsets;
}

SSFileCreator::offset SSFileCreator::writeKeyChunks(std::fstream *stream, const SSFileCreator::KeysBySize &keysBySize, const std::map<std::string, offset> &valueOffsets) {
    auto prevOffset = stream->tellg();
    for (const auto &it : keysBySize){
        auto fixedKeySize = it.first;
        auto keys = it.second;
        auto keyOffsetPairSize = (fixedKeySize + sizeof(offset));
        auto chunkSize = keyOffsetPairSize * keys.size();
        auto chunkHeader = KeyChunkHeader(fixedKeySize, chunkSize);
        writeChunkHeader(stream, chunkHeader);
        for (const auto& key : keys){
            writeKeyOffsetPair(stream, key, valueOffsets.at(key), fixedKeySize);
        }
    }
    return prevOffset;
}

SSFileCreator::KeysBySize SSFileCreator::groupByChunkKeySize(const DbMemCache *memcache) {
    KeysBySize groups;
    memcache->traverseSorted([&](const std::string &key, const DbValue& value) {
        groups[findChunkKeySize(key)].push_back(key);
    });

    return groups;
}

bool SSFileCreator::isFilenameSSTable(const std::filesystem::path &path) {
    return std::regex_match(path.filename().string(), ssTableFilenameRegex);
}


SSFileCreator::offset SSFileCreator::writeValue(std::fstream *stream, const ValueHeader &valueHeader, const DbValue& value) {
    auto offset = writeValueHeader(stream, valueHeader);
    if (valueHeader.isEntryRemoved()){
        return offset;
    }

    auto str = dbValueToString(value);
    stream->write(str.data(), str.size());
    return offset;
}

SSFileCreator::offset SSFileCreator::writePlaceHolderSSFileHeader(std::fstream* stream) {
    auto offset = stream->tellg();
    SSFileHeader header{};
    stream->write(reinterpret_cast<const char*>(&header), sizeof(header));
    return offset;
}

void SSFileCreator::modifySSFileHeader(std::fstream *stream, SSFileCreator::offset headerPos,
                                       const SSFileCreator::SSFileHeader &header) {
    auto oldOffset = stream->tellg();
    stream->seekg(headerPos);
    stream->write(reinterpret_cast<const char*>(&header), sizeof(header));
    stream->seekg(oldOffset);
}

SSFileCreator::offset SSFileCreator::writeValueHeader(std::fstream* stream, const ValueHeader &valueHeader) {
    auto offset = stream->tellg();
    stream->write(reinterpret_cast<const char*>(&valueHeader), sizeof(valueHeader));
    return offset;
}

SSFileCreator::offset SSFileCreator::writeChunkHeader(std::fstream *stream, const KeyChunkHeader &header) {
    auto prevOffset = stream->tellg();
    stream->write(reinterpret_cast<const char*>(&header), sizeof(header));
    return prevOffset;
}

SSFileCreator::offset SSFileCreator::writeKeyOffsetPair(std::fstream *stream, std::string key, offset offset, size_t fixedKeySize) {
    while (key.size() < fixedKeySize){
        key += '\0';
    }

    auto prevOffset = stream->tellg();
    stream->write(key.data(), key.size());
    stream->write(reinterpret_cast<const char*>(&offset), sizeof(offset));
    return prevOffset;
}

size_t SSFileCreator::findChunkKeySize(const std::string &key) {
    int i = 0;
    while (i < chunkKeySizes.size() && key.size() > chunkKeySizes[i]){
        i++;
    }

    return chunkKeySizes[i];
}