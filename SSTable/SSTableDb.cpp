#include "SSTableDb.h"

#include <utility>
#include <algorithm>
#include "csv.hpp"

SSTableDb::SSTableDb(MemCache &memCache, const csv::CSVWriter<std::fstream> &writer, const std::filesystem::path& directory) : memcache(memCache), writeAheadLogWriter(writer), baseDirectory(directory){
    if (!is_directory(directory)){
        throw std::runtime_error("Expected directory, received" + directory.string());
    }

    openWriteAheadLog();
    populateMemcacheFromLog();
    populateSSTables();
}

void SSTableDb::set(const std::string &key, DbValue value) {
    tombstones.erase(key);
    auto entry = Entry(key, std::move(value));
    writeEntryToLog(entry);
    memcache.insert(entry);
    if (shouldFlushMemcache()){
        flushMemcache();
    }
}

std::optional<DbValue> SSTableDb::get(const std::string &key) {
    if (tombstones.find(key) != tombstones.end()){
        return std::nullopt;
    }

    auto cached = memcache.get(key);
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


bool SSTableDb::remove(const std::string &key) {
    writeTombstoneToLog(key);
    tombstones.insert(key);
    memcache.remove(key);
}

void SSTableDb::flushMemcache() {
    auto file = std::make_unique<SSTableFile>(baseDirectory, ssTableFiles.size(), memcache, tombstones);
    ssTableFiles.push_back(std::move(file));
    memcache.clear();
    clearWriteAheadLog();
}

SSTableDb::~SSTableDb() {
    flushMemcache();
}

bool SSTableDb::shouldFlushMemcache() {
    return memcache.size() > maxMemcacheSize;
}

void SSTableDb::populateSSTables() {
    for (const auto& dirEntry : std::filesystem::directory_iterator(baseDirectory)){
        if (dirEntry.is_regular_file() && SSTableFile::isFilenameSSTable(dirEntry.path().filename())){
            ssTableFiles.push_back(std::make_unique<SSTableFile>(dirEntry.path()));
        }
    }

    std::sort(ssTableFiles.begin(), ssTableFiles.end(), [] (const std::unique_ptr<SSTableFile>& lhs, const std::unique_ptr<SSTableFile> &rhs){
        return lhs->index < rhs->index;
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
}

void SSTableDb::processWriteAheadLogLine(csv::CSVRow &row) {
    auto key = row["key"].get<std::string>();
    if (row["tombstone"].get<int>()){
        tombstones.insert(key);
        memcache.remove(key);
        return;
    }

    tombstones.erase(key);
    auto value_type_index = row["value_type"].get<int>();
    Entry entry(key, value_type_index, row["value"].get<std::string>());
    memcache.insert(entry);
}

void SSTableDb::writeEntryToLog(const Entry &entry) {
    writeAheadLog.seekg(0, std::ios::end);
    writeAheadLogWriter << std::vector<std::string>({"1", entry.key, std::to_string(entry.value.index()), entry.valueToString()});
}

void SSTableDb::writeTombstoneToLog(const std::string &key) {
    writeAheadLog.seekg(0, std::ios::end);
    writeAheadLogWriter << std::vector<std::string>({"0", key});
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

