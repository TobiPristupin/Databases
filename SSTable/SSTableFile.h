#ifndef DATAINTENSIVE_SSTABLEFILE_H
#define DATAINTENSIVE_SSTABLEFILE_H

#include <filesystem>
#include <fstream>
#include <unordered_set>
#include "DatabaseEntry.h"
#include "MemCache.h"


class SSTableFile {
public:
    size_t index;

    SSTableFile(const std::filesystem::path &directory, int index, MemCache &memcache, const std::unordered_set<std::string>& tombstones);
    explicit SSTableFile(const std::filesystem::path &file);
    std::optional<DbValue> get(const std::string &key) const;

    static bool isFilenameSSTable(const std::filesystem::path &filename);
    static std::optional<int> extractIndexFromFilename(const std::filesystem::path &filename);
private:
    const std::string ssTableFilenameFormat = "sstable_{}.db";
    const std::string ssTableFilenameRegex = "^sstable_(?<index>\\d+)\\.db$";
    std::fstream file;

    void writeToFile(MemCache &memcache, const std::unordered_set<std::string>& tombstones);
};


#endif
