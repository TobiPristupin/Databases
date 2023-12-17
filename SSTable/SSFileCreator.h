#ifndef DATAINTENSIVE_SSFILECREATOR_H
#define DATAINTENSIVE_SSFILECREATOR_H

#include <filesystem>
#include <fstream>
#include <set>
#include "../DatabaseEntry.h"
#include <regex>
#include "MemCache.h"
#include "SSFile.h"
#include "DbMemCache.h"
#include "SSTableParams.h"


class SSFileCreator {
public:
    static std::unique_ptr<SSFile> newFile(const std::filesystem::path &directory, size_t index, uint32_t filterBits, const DbMemCache *memcache, const std::set<std::string>& tombstones);
    static std::unique_ptr<SSFile> loadFile(const std::filesystem::path &file);
    static bool isFilenameSSTable(const std::filesystem::path &path);

private:

    using KeysBySize = std::map<size_t, std::vector<std::string>>;
    using offset = SSFile::offset;
    using SSFileHeader = SSFile::SSFileHeader;
    using ValueHeader = SSFile::ValueHeader;
    using KeyChunkHeader = SSFile::KeyChunkHeader;

    inline static const std::string ssTableFilenameFormat = "sstable_{}.db";
    inline static const std::regex ssTableFilenameRegex = std::regex("^sstable_(\\d+).db$");
    inline static const std::vector<size_t> chunkKeySizes = {8, 16, 32, 64, 128, 256, 512, SSTable::maxKeySize};
    static_assert(SSTable::maxKeySize > 512, "Max key size must be larger than the previous key chunk size. Adjust key chunk sizes if changing max key size.");

    static offset writePlaceHolderSSFileHeader(std::fstream* stream);
    static void modifySSFileHeader(std::fstream* stream, offset headerPos, const SSFileHeader &header);
    static offset writeToFile(std::fstream* stream, const DbMemCache *memcache, const std::set<std::string>& tombstones, uint32_t bloomFilterLength);
    static offset writeValue(std::fstream* stream, const DbValue& value);
    static offset writeValueHeader(std::fstream* stream, const ValueHeader &valueHeader);
    static offset writeChunkHeader(std::fstream* stream, const KeyChunkHeader &header);
    static offset writeKeyOffsetPair(std::fstream* stream, std::string key, offset offset, size_t fixedKeySize);
    static KeysBySize groupByChunkKeySize(const DbMemCache *memcache, const std::set<std::string>& tombstones);
    static size_t findChunkKeySize(const std::string &key);
    static std::map<std::string, offset> writeValues(std::fstream *stream, const DbMemCache *memcache,
                                                     const std::set<std::string> &tombstones);
    static offset writeKeyChunks(std::fstream *stream, const KeysBySize &keysBySize, const std::map<std::string, offset> &valueoffsets);
};


#endif
