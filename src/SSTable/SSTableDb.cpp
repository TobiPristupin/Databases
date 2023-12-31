#include "SSTableDb.h"

#include <utility>
#include <algorithm>
#include "csv.hpp"

SSTableDb::SSTableDb(std::unique_ptr<DbMemCache> memCache, const std::filesystem::path& directory, bool reset, bool useBloomFilter)
: memcache(std::move(memCache)), baseDirectory(directory), useBloomFilter(useBloomFilter){
    if (!is_directory(directory)){
        throw std::runtime_error("Expected directory, received" + directory.string());
    }

    openWriteAheadLog(reset);
    writeAheadLogWriter = std::make_unique<csv::CSVWriter<std::fstream>>(writeAheadLog);
    populateMemcacheFromLog();
    if (!reset){
        populateSSTables();
    }
}

void SSTableDb::insert(const std::string &key, const DbValue& value) {
    validateKey(key);
    tombstones.erase(key);
    writeEntryToLog(key, value);
    memcache->insert(key, value);
    if (shouldFlushMemcache()){
        flushMemcache();
    }
}

std::optional<DbValue> SSTableDb::get(const std::string &key) {
    if (tombstones.find(key) != tombstones.end()){
        return std::nullopt;
    }

    auto cached = memcache->get(key);
    if (cached.has_value()){
        return cached.value();
    }

    for (auto it = ssTableFiles.rbegin(); it != ssTableFiles.rend(); it++){
        auto read = (*it)->get(key);
        switch (read.type) {
            case KEY_FOUND:
                return read.value.value();
            case KEY_TOMBSTONE:
                return std::nullopt;
            case KEY_NOT_FOUND:
                break;
        }
    }

    return std::nullopt;
}

void SSTableDb::remove(const std::string &key) {
    writeTombstoneToLog(key);
    tombstones.insert(key);
    memcache->remove(key);
}

void SSTableDb::flushMemcache() {
    if (memcache->size() == 0){
        return;
    }

    auto newIndex = ssTableFiles.size();
    uint32_t filterBits = useBloomFilter ? SSTable::bloomFilterBits : 0;
    auto file = SSFileCreator::newFile(baseDirectory / ssTablesDirectory, newIndex, filterBits, memcache.get(), tombstones);
    ssTableFiles.push_back(std::move(file));
    memcache->clear();
    clearWriteAheadLog();
}

SSTableDb::~SSTableDb() {
    flushMemcache();
}

bool SSTableDb::shouldFlushMemcache() {
    return memcache->size() >= SSTable::maxMemcacheSize;
}

void SSTableDb::populateSSTables() {
    for (const auto& dirEntry : std::filesystem::directory_iterator(baseDirectory / ssTablesDirectory)){
        if (dirEntry.is_regular_file() && SSFileCreator::isFilenameSSTable(dirEntry.path().filename())){
            ssTableFiles.push_back(SSFileCreator::loadFile(dirEntry.path()));
        }
    }

    std::sort(ssTableFiles.begin(), ssTableFiles.end(), [] (const std::unique_ptr<SSFile>& lhs, const std::unique_ptr<SSFile> &rhs){
        return lhs->getIndex() < rhs->getIndex();
    });
}

void SSTableDb::populateMemcacheFromLog() {
    writeAheadLog.seekg(0);
    std::vector<std::string> col_names = {"tombstone", "key", "value_type", "value"};
    csv::CSVFormat format;
    format.column_names(col_names);
    csv::CSVReader reader(writeAheadLog, format);
    for (csv::CSVRow &row : reader){
        processWriteAheadLogLine(row);
    }

    //CSV reader closes the file after reading it. We need to keep it open to write to it
    openWriteAheadLog(false);
}

void SSTableDb::processWriteAheadLogLine(csv::CSVRow &row) {
    auto key = row["key"].get<std::string>();
    if (row["tombstone"].get<int>()){
        tombstones.insert(key);
        memcache->remove(key);
        return;
    }

    tombstones.erase(key);
    auto valueTypeIndex = row["value_type"].get<int>();
    auto valueStr = row["value"].get<std::string>();
    memcache->insert(key, dbValueFromString(valueTypeIndex, valueStr));
}

void SSTableDb::writeEntryToLog(const std::string &key, const DbValue &value) {
    writeAheadLog.seekg(0, std::ios::end);
    *writeAheadLogWriter << std::vector<std::string>({"0", key, std::to_string(value.index()), dbValueToString(value)});
}

void SSTableDb::writeTombstoneToLog(const std::string &key) {
    writeAheadLog.seekg(0, std::ios::end);
    *writeAheadLogWriter << std::vector<std::string>({"1", key, "0", "0"});
}

void SSTableDb::openWriteAheadLog(bool reset) {
    auto mode = std::ios::in | std::ios::out;
    if (reset){
        mode |= std::ios::trunc;
    } else {
        mode |= std::ios::app;
    }

    writeAheadLog.open(baseDirectory / writeAheadLogFilename, mode);
}

void SSTableDb::clearWriteAheadLog() {
    writeAheadLog.close();
    openWriteAheadLog(true);
}

void SSTableDb::validateKey(const std::string &key) {
    // This validation is required by the implementation of SSTableFiles.
    if (key.find('\0') != std::string::npos){
        throw std::runtime_error("Cannot insert keys containing the '\\0' character. Attempted to insert key " + key);
    }

    if (key.size() > SSTable::maxKeySize){
        throw std::runtime_error("Cannot insert keys with more than " + std::to_string(SSTable::maxKeySize) + " characters.");
    }
}


