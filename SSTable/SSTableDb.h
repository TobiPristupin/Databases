#ifndef DATAINTENSIVE_SSTABLEDB_H
#define DATAINTENSIVE_SSTABLEDB_H

#include <fstream>
#include <unordered_set>
#include "../DatabaseEntry.h"
#include "MemCache.h"
#include "BST.hpp"
#include "SSFileCreator.h"
#include "csv.hpp"

class SSTableDb {
public:
    explicit SSTableDb(std::unique_ptr<DbMemCache> memCache, const std::filesystem::path& directory = ".", bool reset=false, bool useBloomFilter=false);
    void insert(const std::string &key, const DbValue& value);
    std::optional<DbValue> get(const std::string &key);
    void remove(const std::string &key);
    ~SSTableDb();

private:
    std::filesystem::path baseDirectory;
    std::unique_ptr<DbMemCache> memcache;
    std::unordered_set<std::string> tombstones;
    bool useBloomFilter;
    const std::filesystem::path writeAheadLogFilename = "write_ahead_log.csv";
    const std::filesystem::path ssTablesDirectory = "sstables";
    std::fstream writeAheadLog;
    std::unique_ptr<csv::CSVWriter<std::fstream>> writeAheadLogWriter;
    std::vector<std::unique_ptr<SSFile>> ssTableFiles;


    bool shouldFlushMemcache();
    void flushMemcache();
    void populateMemcacheFromLog();
    void populateSSTables();
    void writeEntryToLog(const std::string &key, const DbValue &value);
    void writeTombstoneToLog(const std::string &key);
    void clearWriteAheadLog();
    void openWriteAheadLog(bool reset=false);
    void processWriteAheadLogLine(csv::CSVRow &row);
    static void validateKey(const std::string &key);
};

#endif
