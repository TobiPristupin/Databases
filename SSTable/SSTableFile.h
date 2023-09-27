#ifndef DATAINTENSIVE_SSTABLEFILE_H
#define DATAINTENSIVE_SSTABLEFILE_H

#include <filesystem>
#include <fstream>
#include <unordered_set>
#include "DatabaseEntry.h"
#include "AvlTree.h"

class SSTableFile {
public:
    SSTableFile(const std::filesystem::path &path, const AvlTree &memcache, const std::unordered_set<std::string> tombstones);
    std::optional<DbValue> get(const std::string &key) const;
private:
    std::fstream file;
};


#endif
