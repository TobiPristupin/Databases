#include <iostream>
#include <benchmark/benchmark.h>
#include "SSTable/MemCache.h"
#include "SSTable/SSTableDb.h"
#include "LogDatabase/LogDatabase.h"
#include "Workload.h"
#include "SSTable/SSTableParams.h"
/*
 * Ideas for benchmarking
 *
 * 1) Workload of random read, writes, deletes
 *  - Record number of reads/writes/deletes, and percentiles for each
 *
 * 2) Comparison of reads in memcache vs reads in SSFile
 * - Fill the memcache with 1024 keys, and then flush the memcache, moving everything to a SSFile. Perform the
 *  same sequence of reads to each and determine the time difference.
 */

static const unsigned long seed = 1;

static void processAction(const Action& action, SSTableDb& ssTableDb){
    switch (action.operation) {
        case Operation::GET:
            ssTableDb.get(action.key);
            break;
        case Operation::INSERT:
            ssTableDb.insert(action.key, action.value);
            break;
        case Operation::DELETE:
            ssTableDb.remove(action.key);
            break;
    }
}

static void processAction(const Action& action, LogDatabase& logDatabase){
    switch (action.operation) {
        case Operation::GET:
            logDatabase.get(action.key);
            break;
        case Operation::INSERT:
            logDatabase.insert(action.key, action.value);
            break;
        case Operation::DELETE:
            logDatabase.remove(action.key);
            break;
    }
}

static void random_workload_log_database(benchmark::State& state){
    WorkloadGenerator workloadGenerator(seed, maxKeySize);
    for (auto _ : state){
        auto workload = workloadGenerator.generateRandomWorkload(100000, 1);
        LogDatabase logDatabase(true);
        for (const auto &action : workload){
            processAction(action, logDatabase);
        }
    }
}

static void random_workload_sstable(benchmark::State& state){
    WorkloadGenerator workloadGenerator(seed, maxKeySize);
    for (auto _ : state){
        auto workload = workloadGenerator.generateRandomWorkload(100000, 1);
        std::unique_ptr<DbMemCache> memCache(new BST<std::string, DbValue>);
        SSTableDb ssTableDb(std::move(memCache), "/home/pristu/Documents/School/DataIntensive/SSTable", true);
        for (const auto &action : workload){
            processAction(action, ssTableDb);
        }
    }
}

static void only_inserts_log_database(benchmark::State& state){
    WorkloadGenerator workloadGenerator(seed, maxKeySize);
    for (auto _ : state){
        auto workload = workloadGenerator.onlyInsertsWorkload(50000);
        LogDatabase logDatabase(true);
        for (const auto &action : workload){
            processAction(action, logDatabase);
        }
    }
}

static void only_inserts_sstable(benchmark::State& state){
    WorkloadGenerator workloadGenerator(seed, maxKeySize);
    for (auto _ : state){
        auto workload = workloadGenerator.onlyInsertsWorkload(50000);
        std::unique_ptr<DbMemCache> memCache(new BST<std::string, DbValue>);
        SSTableDb ssTableDb(std::move(memCache), "/home/pristu/Documents/School/DataIntensive/SSTable", true);
        for (const auto &action : workload){
            processAction(action, ssTableDb);
        }
    }
}

// TODO: refactor benchmarks to take a database interface instead of having a benchmark per database
// TODO: use a benchmark fixture to avoid setting up workload generator each benchmark?
BENCHMARK(random_workload_log_database);
BENCHMARK(random_workload_sstable);
BENCHMARK(only_inserts_log_database);
BENCHMARK(only_inserts_sstable);


BENCHMARK_MAIN();

/*
 * TODO
 * If expected actions per key value is high, we end up deleting too much. Need to bias towards getting and inserting.
 *
 * Add SSfile header, read index from header instead of filename
 * add bloom filter
 *
 */