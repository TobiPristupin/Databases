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

    struct KeyValueLengths {
        KeyValueLengths() : dataLength(0), keyLength(0) {};
        KeyValueLengths(uint32_t dataLength, uint16_t keyLength) : dataLength(dataLength), keyLength(keyLength) {};
        KeyValueLengths(const KeyValueLengths& keyValueData) = default;

        uint32_t keyLength;
        uint32_t dataLength;
    };

    std::map<std::string, std::streamoff> offset;
    std::fstream file;
    const std::string filename = "log_database.db";

    void initializeOffsets();
    std::optional<KeyValueLengths> readKeyValueLengths();
    void writeEntry(const KeyValueLengths &keyValueLengths, std::string key, const char* data, std::streamsize size);
    std::string readString(std::streamsize size);
};

LogDatabase::LogDatabase(bool reset) {
    auto mode = std::ios::out | std::ios::in | std::ios::binary;
    if (reset){
        mode |= std::ios::trunc;
    } else {
        mode |= std::ios::app;
    }
    file.open(filename, mode);
    initializeOffsets();
}

template<typename T>
T LogDatabase::get(const std::string &key) {
    static_assert(std::is_trivially_copyable<T>::value, "Types must be trivially copyable");
    if (!contains(key)){
        throw std::runtime_error("Key " + key + " does not exist");
    }

    file.seekg(offset[key], std::ios::beg);
    auto keyValueLengths = readKeyValueLengths().value();
    file.seekg(keyValueLengths.keyLength, std::ios::cur);
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
    auto keyValueLengths = readKeyValueLengths().value();
    file.seekg(keyValueLengths.keyLength, std::ios::cur);
    auto value = readString(keyValueLengths.dataLength);
    file.seekg(0, std::ios::end);
    return value;
}

template<typename T>
void LogDatabase::set(const std::string &key, const T& data) {
    static_assert(std::is_trivially_copyable<T>::value, "Types must be trivially copyable");
    offset[key] = file.tellg();
    auto keyValueLengths = KeyValueLengths(sizeof(data), key.size());
    writeEntry(keyValueLengths, key, reinterpret_cast<const char*>(&data), sizeof(data));
}

template<>
void LogDatabase::set<std::string>(const std::string &key, const std::string &data){
    offset[key] = file.tellg();
    auto keyValueLengths = KeyValueLengths(data.size(), key.size());
    writeEntry(keyValueLengths, key, data.data(), data.size());
}

bool LogDatabase::contains(const std::string& key) {
    return offset.find(key) != offset.end();
}

bool LogDatabase::remove(const std::string &key) {
    return offset.erase(key) > 0;
}

void LogDatabase::initializeOffsets() {
    std::streamoff file_pointer = 0;
    auto keyValueLengths = readKeyValueLengths();
    while (keyValueLengths.has_value()){
        auto key = readString(keyValueLengths.value().keyLength);
        offset[key] = file_pointer;
        file.seekg(keyValueLengths.value().dataLength, std::ios::cur);
        file_pointer = file.tellg();
        keyValueLengths = readKeyValueLengths();
    }
}

std::optional<LogDatabase::KeyValueLengths> LogDatabase::readKeyValueLengths() {
    KeyValueLengths keyValueLengths;
    if (file.readsome((char*)(&keyValueLengths), sizeof(keyValueLengths))){
        return keyValueLengths;
    }
    return std::nullopt;
}

void LogDatabase::writeEntry(const LogDatabase::KeyValueLengths &keyValueLengths, std::string key, const char *data, std::streamsize size) {
    file.write(reinterpret_cast<const char*>(&keyValueLengths), sizeof(keyValueLengths));
    file.write(key.data(), key.size());
    file.write(data, size);
}

std::string LogDatabase::readString(std::streamsize size) {
    std::vector<char> buffer(size);
    file.read(buffer.data(), size);
    return {buffer.begin(), buffer.end()};
}



#endif //DATAINTENSIVE_LOGDATABASE_H
