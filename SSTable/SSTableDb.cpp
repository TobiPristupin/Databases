#include "SSTableDb.h"

#include <utility>
#include <algorithm>
#include "csv.hpp"

SSTableDb::SSTableDb(std::unique_ptr<DbMemCache> memCache, const std::filesystem::path& directory) : memcache(std::move(memCache)), baseDirectory(directory){
    if (!is_directory(directory)){
        throw std::runtime_error("Expected directory, received" + directory.string());
    }

    openWriteAheadLog();
    writeAheadLogWriter = std::make_unique<csv::CSVWriter<std::fstream>>(writeAheadLog);
    populateMemcacheFromLog();
    populateSSTables();
}

void SSTableDb::set(const std::string &key, const DbValue& value) {
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

    for (const auto& ssTable : ssTableFiles){
        auto value = ssTable->get(key);
        if (value.has_value()){
            return value.value();
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
    auto file = SSFileCreator::newFile(baseDirectory / ssTablesDirectory, ssTableFiles.size(), memcache.get(), tombstones);
    ssTableFiles.push_back(std::move(file));
    memcache->clear();
    clearWriteAheadLog();
}

SSTableDb::~SSTableDb() {
    flushMemcache();
}

bool SSTableDb::shouldFlushMemcache() {
    return memcache->size() > maxMemcacheSize;
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

    if (key.size() > maxKeySize){
        throw std::runtime_error("Cannot insert keys with more than " + std::to_string(maxKeySize) + " characters.");
    }
}


