#ifndef DATAINTENSIVE_SSFILE_H
#define DATAINTENSIVE_SSFILE_H

#include <cstdint>
#include <ios>
#include <vector>
#include <fstream>
#include "DatabaseEntry.h"

class SSFile {
public:

    using offset = std::streamoff;

    struct ValueHeader {
        ValueHeader();
        ValueHeader(uint32_t dataLength, uint32_t typeIndex);
        bool isEntryRemoved() const;

        /*
         * Note: dataLength = 0 when the entry is removed. This is an
         * optimization, so we don't need to have a separate "removed"
         * field.
         */
        uint32_t dataLength;

        /*
         * Corresponds to the type index of DbValue's variant type
         */
        size_t typeIndex;
    };

    struct KeyChunkHeader {
        KeyChunkHeader();
        KeyChunkHeader(uint32_t fixedKeySize, uint32_t chunkLength);

        uint32_t getFixedKeySize() const;
        uint32_t getLength() const;
        size_t keyOffsetPairLength() const;
        size_t getNumKeysInChunk() const;

    private:
        uint32_t fixedKeySize;
        uint32_t length;
    };

    struct KeyOffsetPair {
        KeyOffsetPair(std::string key, offset pos);

        std::string key;
        offset pos;
    };

    SSFile(std::fstream file, size_t index, offset keyFooterStart);
    std::optional<DbValue> get(const std::string &key);
    size_t getIndex() const;

private:

    std::fstream file;
    SSFile::offset keyFooterStart;
    size_t index;

    KeyChunkHeader moveToChunkForKey(const std::string &key);
    KeyChunkHeader readKeyChunkHeader();
    std::optional<offset> findValueOffset(offset chunkStart, KeyChunkHeader chunkHeader, const std::string &key);
    KeyOffsetPair readKeyOffsetPair(size_t fixedKeySize);
    DbValue readValue();
    ValueHeader readValueHeader();

};


#endif
