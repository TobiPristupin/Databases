#ifndef DATAINTENSIVE_SSFILE_H
#define DATAINTENSIVE_SSFILE_H

#include <cstdint>
#include <ios>
#include <vector>
#include <fstream>
#include <optional>
#include "../DatabaseEntry.h"
#include "BloomFilter.h"

/*
 * Structure of an SSFile is as follows:
 *
 * SSFileHeader
 * [Optional] bloomFilterBits
 * Values
 * [One or more] KeyChunk
 *
 * Where each KeyChunk is as follows:
 *
 * KeyChunkHeader
 * [One or more] (Key, value offset) pairs
 */

enum SSFileReadType {
    KEY_FOUND, KEY_TOMBSTONE, KEY_NOT_FOUND
};

struct SSFileRead {
    SSFileReadType type;
    std::optional<DbValue> value;
};

class SSFile {
public:

    using offset = std::streamoff;

    SSFile(std::fstream file);
    SSFileRead get(const std::string &key);
    size_t getIndex() const;

private:

    struct SSFileHeader {
        SSFileHeader() = default;
        SSFileHeader(uint32_t index, uint32_t bloomFilterLength, uint32_t footerStart);

        uint32_t index;
        uint32_t filterBits;
        uint32_t keyFooterStart;

        bool hasBloomFilter() const;
        size_t bloomFilterLength() const;
    };

    struct ValueHeader {
        ValueHeader() = default;
        ValueHeader(uint32_t dataLength, DbValueTypeIndex typeIndex);
        static ValueHeader TombstoneHeader();
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
        DbValueTypeIndex typeIndex;
    };

    struct KeyChunkHeader {
        KeyChunkHeader() = default;
        KeyChunkHeader(uint32_t fixedKeySize, uint32_t chunkLength);


        uint32_t fixedKeySize;
        uint32_t length;

        size_t keyOffsetPairLength() const;
        size_t getNumKeysInChunk() const;
    };

    struct KeyOffsetPair {
        KeyOffsetPair(std::string key, offset pos);

        std::string key;
        offset pos;
    };

    std::fstream file;
    SSFileHeader header{};
    std::optional<BloomFilter> bloomFilter;

    SSFileHeader readSSFileHeader();
    BloomFilter readBloomFilter(uint32_t bloomFilterLength);
    KeyChunkHeader moveToChunkForKey(const std::string &key);
    KeyChunkHeader readKeyChunkHeader();
    std::optional<offset> findValueOffset(offset chunkStart, KeyChunkHeader chunkHeader, const std::string &key);
    KeyOffsetPair readKeyOffsetPair(size_t fixedKeySize);
    DbValue readValue(const ValueHeader &header);
    ValueHeader readValueHeader();

    friend class SSFileCreator;
};


#endif
