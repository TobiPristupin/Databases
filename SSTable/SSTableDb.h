#ifndef DATAINTENSIVE_SSTABLEDB_H
#define DATAINTENSIVE_SSTABLEDB_H

#include <fstream>
#include <unordered_set>
#include "DatabaseEntry.h"
#include "MemCache.h"
#include "AvlTree.h"
#include "SSTableFile.h"
#include "csv.hpp"

class SSTableDb {
public:
    explicit SSTableDb(MemCache &memCache, const csv::CSVWriter<std::fstream> &writer, const std::filesystem::path&  directory = ".");
    void set(const std::string &key, DbValue value);
    std::optional<DbValue> get(const std::string &key);
    bool remove(const std::string &key);

private:
    std::filesystem::path baseDirectory;
    MemCache& memcache;
    const size_t maxMemcacheSize = 1024;
    std::unordered_set<std::string> tombstones;
    const std::filesystem::path writeAheadLogFilename = "write_ahead_log.csv";
    std::fstream writeAheadLog;
    csv::CSVWriter<std::fstream> writeAheadLogWriter;
    std::vector<std::unique_ptr<SSTableFile>> ssTableFiles;


    bool shouldFlushMemcache();
    void flushMemcache();
    void populateMemcacheFromLog();
    void populateSSTables();
    void writeEntryToLog(const Entry &entry);
    void writeTombstoneToLog(const std::string &key);
    void clearWriteAheadLog();
    void openWriteAheadLog(bool reset=false);
    ~SSTableDb();

    void processWriteAheadLogLine(csv::CSVRow &row);
};

#endif
