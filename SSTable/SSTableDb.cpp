#include "SSTableDb.h"

#include <utility>
#include <format>

SSTableDb::SSTableDb(const std::filesystem::path& directory)  {
    if (!is_directory(directory)){
        throw std::runtime_error("Expected directory, received" + directory.string());
    }

    baseDirectory = directory;
    writeAheadLog.open(baseDirectory / writeAheadLogFilename, std::ios::in | std::ios::out | std::ios::app);
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
    auto filename = std::format();
//    auto filename = "sstable_" + std::to_string(ssTableFiles.size()) + ".db";
    auto file = std::make_unique<SSTableFile>(baseDirectory / filename, memcache, tombstones);
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
        if (dirEntry.is_regular_file() && dirEntry.path().filename() == ""){

        }
    }
}

void SSTableDb::populateMemcacheFromLog() {

}

void SSTableDb::clearWriteAheadLog() {

}

void SSTableDb::writeEntryToLog(const Entry &entry) {

}

void SSTableDb::writeTombstoneToLog(const std::string &key) {

}