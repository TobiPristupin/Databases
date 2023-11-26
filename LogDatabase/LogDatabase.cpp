#include "LogDatabase.h"

LogDatabase::LogDatabase(bool reset) {
    openFile(reset);
    initializeOffsets();
}

std::optional<DbValue> LogDatabase::get(const std::string &key) {
    if (!contains(key)){
        return std::nullopt;
    }

    file.seekg(offset[key], std::ios::beg);
    auto entryHeader = readEntryHeader().value();

    if (entryHeader.isTombstone()){
        return std::nullopt;
    }

    file.seekg(entryHeader.keyLength, std::ios::cur);

    std::vector<char> data(entryHeader.dataLength, 0);
    file.read(data.data(), entryHeader.dataLength);
    return dbValueFromString(entryHeader.typeIndex, std::string(data.begin(), data.end()));
}

void LogDatabase::insert(const std::string &key, const DbValue& value) {
    file.seekg(0, std::ios::end);
    offset[key] = file.tellg();
    auto entryHeader = EntryHeader(dbValueToString(value).size(), key.size(), value.index());
    writeEntry(entryHeader, key, value);
}

bool LogDatabase::contains(const std::string& key) {
    return offset.find(key) != offset.end();
}

bool LogDatabase::remove(const std::string &key) {
    if (!contains(key)){
        return false;
    }

    file.seekg(0, std::ios::end);
    auto newHeader = EntryHeader::tombstoneHeader(key.size());
    writeTombstone(newHeader, key);
    offset.erase(key);
    return true;
}

void LogDatabase::initializeOffsets() {
    std::streamoff file_pointer = 0;
    auto entryHeader = readEntryHeader();
    while (entryHeader.has_value()){
        auto key = readString(entryHeader.value().keyLength);
        if (entryHeader->isTombstone()){
            offset.erase(key);
            continue;
        }


        offset[key] = file_pointer;
        file.seekg(entryHeader.value().dataLength, std::ios::cur);
        file_pointer = file.tellg();
        entryHeader = readEntryHeader();
    }
}

std::optional<LogDatabase::EntryHeader> LogDatabase::readEntryHeader() {
    EntryHeader entryHeader;
    if (file.readsome((char*)(&entryHeader), sizeof(entryHeader))){
        return entryHeader;
    }
    return std::nullopt;
}

void LogDatabase::writeEntry(const LogDatabase::EntryHeader &header, std::string key, const DbValue& value) {
    writeHeader(header);
    file.write(key.data(), key.size());
    auto str = dbValueToString(value);
    file.write(str.data(), str.size());

    if (file.tellg() > fileSizeTriggerCompaction){
        compactFile();
    }
}

void LogDatabase::writeTombstone(const LogDatabase::EntryHeader &header, std::string key) {
    writeHeader(header);
    file.write(key.data(), key.size());

    if (file.tellg() > fileSizeTriggerCompaction){
        compactFile();
    }
}

void LogDatabase::writeHeader(const LogDatabase::EntryHeader &entryHeader) {
    file.write(reinterpret_cast<const char*>(&entryHeader), sizeof(entryHeader));
}

void LogDatabase::openFile(bool reset) {
    auto mode = std::ios::out | std::ios::in | std::ios::binary;
    if (reset){
        mode |= std::ios::trunc;
    } else {
        mode |= std::ios::app;
    }
    file.open(filename, mode);
}

void LogDatabase::compactFile() {
    //TODO
}

std::string LogDatabase::readString(std::streamsize size) {
    std::vector<char> buffer(size);
    file.read(buffer.data(), size);
    return {buffer.begin(), buffer.end()};
}

LogDatabase::EntryHeader::EntryHeader() : keyLength(0), dataLength(0), typeIndex(0) {}

LogDatabase::EntryHeader::EntryHeader(uint32_t dataLength, uint32_t keyLength, DbValueTypeIndex typeIndex)
    : dataLength(dataLength), keyLength(keyLength), typeIndex(typeIndex) {}

bool LogDatabase::EntryHeader::isTombstone() const {
    return dataLength == 0;
}

LogDatabase::EntryHeader LogDatabase::EntryHeader::tombstoneHeader(uint32_t keyLength) {
    return {0, keyLength, 0};
}
