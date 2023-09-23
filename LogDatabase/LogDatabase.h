#ifndef DATAINTENSIVE_LOGDATABASE_H
#define DATAINTENSIVE_LOGDATABASE_H

#include <string>
#include <map>
#include <fstream>
#include <vector>
#include <optional>

class LogDatabase {
public:
    explicit LogDatabase(bool reset = false);

    template<typename T>
    void set(const std::string &key, const T& data);

    template<typename T>
    T get(const std::string &key);

    bool remove(const std::string &key);

    bool contains(const std::string& key);

private:

    struct EntryHeader {
        EntryHeader() : dataLength(0), keyLength(0), deleted(false) {};
        EntryHeader(uint32_t dataLength, uint16_t keyLength, bool deleted=false) : dataLength(dataLength), keyLength(keyLength), deleted(deleted) {};
        EntryHeader(const EntryHeader& entryHeader) = default;

        uint32_t keyLength;
        uint32_t dataLength;
        bool deleted;
    };

    const std::string filename = "log_database.db";
    size_t fileSizeTriggerCompaction = 2048;
    std::map<std::string, std::streamoff> offset;
    std::fstream file;


    void initializeOffsets();
    std::optional<EntryHeader> readEntryHeader();
    void writeEntry(const EntryHeader &entryHeader, std::string key, const char* data, std::streamsize size);
    void writeHeader(const EntryHeader &entryHeader);
    void compactFile();
    void openFile(bool reset);
    std::string readString(std::streamsize size);
};

LogDatabase::LogDatabase(bool reset) {
    openFile(reset);
    initializeOffsets();
}

template<typename T>
T LogDatabase::get(const std::string &key) {
    static_assert(std::is_trivially_copyable<T>::value, "Types must be trivially copyable");
    if (!contains(key)){
        throw std::runtime_error("Key " + key + " does not exist");
    }

    file.seekg(offset[key], std::ios::beg);
    auto entryHeader = readEntryHeader().value();
    if (entryHeader.deleted){
        throw std::runtime_error("Key " + key + " does not exist");
    }
    file.seekg(entryHeader.keyLength, std::ios::cur);
    T t;
    file.read(reinterpret_cast<char*>(&t), sizeof(T));
    file.seekg(0, std::ios::end);
    return t;
}

template<>
std::string LogDatabase::get(const std::string &key) {
    if (!contains(key)){
        throw std::runtime_error("Key " + key + " does not exist");
    }

    file.seekg(offset[key], std::ios::beg);
    auto entryHeader = readEntryHeader().value();
    if (entryHeader.deleted){
        throw std::runtime_error("Key " + key + " does not exist");
    }
    file.seekg(entryHeader.keyLength, std::ios::cur);
    auto value = readString(entryHeader.dataLength);
    file.seekg(0, std::ios::end);
    return value;
}

template<typename T>
void LogDatabase::set(const std::string &key, const T& data) {
    static_assert(std::is_trivially_copyable<T>::value, "Types must be trivially copyable");
    offset[key] = file.tellg();
    auto entryHeader = EntryHeader(sizeof(data), key.size());
    writeEntry(entryHeader, key, reinterpret_cast<const char*>(&data), sizeof(data));
}

template<>
void LogDatabase::set<std::string>(const std::string &key, const std::string &data){
    offset[key] = file.tellg();
    auto entryHeader = EntryHeader(data.size(), key.size());
    writeEntry(entryHeader, key, data.data(), data.size());
}

bool LogDatabase::contains(const std::string& key) {
    return offset.find(key) != offset.end();
}

bool LogDatabase::remove(const std::string &key) {
    if (!contains(key)){
        return false;
    }

    file.seekg(offset[key], std::ios::beg);
    auto entryHeader = readEntryHeader().value();
    entryHeader.deleted = true;
    file.seekg(offset[key], std::ios::beg);
    writeHeader(entryHeader);
    offset.erase(key);
    file.seekg(0, std::ios::end);
    return true;
}

void LogDatabase::initializeOffsets() {
    std::streamoff file_pointer = 0;
    auto entryHeader = readEntryHeader();
    while (entryHeader.has_value()){
        auto key = readString(entryHeader.value().keyLength);
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

void LogDatabase::writeEntry(const LogDatabase::EntryHeader &entryHeader, std::string key, const char *data, std::streamsize size) {
    writeHeader(entryHeader);
    file.write(key.data(), key.size());
    file.write(data, size);

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



#endif //DATAINTENSIVE_LOGDATABASE_H
