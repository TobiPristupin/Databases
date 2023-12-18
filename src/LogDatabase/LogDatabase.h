#ifndef DATAINTENSIVE_LOGDATABASE_H
#define DATAINTENSIVE_LOGDATABASE_H

#include <string>
#include <map>
#include <fstream>
#include <vector>
#include <optional>
#include "../KeyValueDb.h"
#include "../DatabaseEntry.h"

class LogDatabase : public KeyValueDb<std::string, DbValue> {
public:
    explicit LogDatabase(bool reset = false);

    void insert(const std::string &key, const DbValue& value) override;
    std::optional<DbValue> get(const std::string &key) override;
    void remove(const std::string &key) override;
    bool contains(const std::string& key);

private:

    struct EntryHeader {
        EntryHeader();
        EntryHeader(uint32_t dataLength, uint32_t keyLength, DbValueTypeIndex typeIndex);
        EntryHeader(const EntryHeader& entryHeader) = default;

        static EntryHeader tombstoneHeader(uint32_t keyLength);

        bool isTombstone() const;

        uint32_t keyLength;

        /*
         * Note: dataLength = 0 when the entry is removed. This is an
         * optimization, so we don't need to have a separate "removed"
         * field.
         */
        uint32_t dataLength;

        /*
         * Corresponds to the type index of DbValue's variant type
         */
        DbValueTypeIndex typeIndex;
    };

    const std::string filename = "log_database.db";
    size_t fileSizeTriggerCompaction = 2048;
    std::map<std::string, std::streamoff> offset;
    std::fstream file;


    void initializeOffsets();
    std::optional<EntryHeader> readEntryHeader();
    void writeEntry(const EntryHeader &entryHeader, std::string key, const DbValue& value);
    void writeTombstone(const EntryHeader &header, std::string key);
    void writeHeader(const EntryHeader &entryHeader);
    void compactFile();
    void openFile(bool reset);
    std::string readString(std::streamsize size);
};

#endif
