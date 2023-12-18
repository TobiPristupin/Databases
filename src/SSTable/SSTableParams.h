#ifndef DATAINTENSIVE_SSTABLEPARAMS_H
#define DATAINTENSIVE_SSTABLEPARAMS_H

namespace SSTable {
    constexpr int maxKeySize = 1024;
    constexpr int maxMemcacheSize = 4096;

/*
 * A maxMemcacheSize of 4096 and bloomFilterBits of 20000 gives us a minimal false positive rate of 9.6% when
 * bloomFilterHashes is 3
 */
    constexpr int bloomFilterBits = 20'000;
    constexpr int bloomFilterHashes = 3;
}


#endif
