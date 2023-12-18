#include <iostream>
#include <benchmark/benchmark.h>
#include "SSTable/MemCache.h"
#include "SSTable/SSTableDb.h"
#include "LogDatabase/LogDatabase.h"
#include "Workload.h"
#include "SSTable/SSTableParams.h"
#include "SSTable/SortedMap.hpp"

/*
 * Ideas for benchmarking
 *
 * 1) Workload of random read, writes, deletes
 *  - Record number of reads/writes/deletes, and percentiles for each
 *
 */

static const unsigned long seed = 1;
static const std::string sstableDirectory = "/home/pristu/Documents/School/DataIntensive/src/SSTable/";

class Fixture : public benchmark::Fixture {
public:

    Fixture(){
        workloadGenerator = std::make_unique<WorkloadGenerator>(seed, SSTable::maxKeySize);
    }

    void SetUp(const ::benchmark::State& state) override {}

    void TearDown(const ::benchmark::State& state) override {}

    static void run_workload(KeyValueDb<std::string, DbValue> &keyValueDb, const std::vector<Action>& workload){
        for (const auto &action : workload){
            processAction(action, keyValueDb);
        }

    }

    static void processAction(const Action& action, KeyValueDb<std::string, DbValue>& logDatabase){
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

    std::unique_ptr<WorkloadGenerator> workloadGenerator;
};

BENCHMARK_F(Fixture, random_workload_log_database)(benchmark::State& state){
    LogDatabase db(true);
    for (auto _ : state) run_workload(db, workloadGenerator->generateRandomWorkload(200000, 10));
}

BENCHMARK_F(Fixture, random_workload_sstable_bst_nofilter)(benchmark::State& state) {
    std::unique_ptr<DbMemCache> memCache(new BST<std::string, DbValue>);
    SSTableDb db(std::move(memCache), sstableDirectory, true, false);
    for (auto _ : state) run_workload(db, workloadGenerator->generateRandomWorkload(200000, 10));
}

BENCHMARK_F(Fixture, random_workload_sstable_rbtree_nofilter)(benchmark::State& state){
    std::unique_ptr<DbMemCache> memCache(new SortedMap<std::string, DbValue>);
    SSTableDb db(std::move(memCache), sstableDirectory, true, false);
    for (auto _ : state) run_workload(db, workloadGenerator->generateRandomWorkload(200000, 10));
}

BENCHMARK_F(Fixture, random_workload_sstable_bst_filter)(benchmark::State& state){
    std::unique_ptr<DbMemCache> memCache(new BST<std::string, DbValue>);
    SSTableDb db(std::move(memCache), sstableDirectory, true, true);
    for (auto _ : state) run_workload(db, workloadGenerator->generateRandomWorkload(200000, 10));
}

BENCHMARK_F(Fixture, sstable_read_from_memcache_bst)(benchmark::State &state){
    std::unique_ptr<DbMemCache> memCache(new BST<std::string, DbValue>);
    SSTableDb db(std::move(memCache), sstableDirectory, true, true);
    auto workload = workloadGenerator->onlyInsertsWorkload(SSTable::maxMemcacheSize - 1);
    run_workload(db, workload);
    for (auto action : workload){
        action.operation = Operation::GET;
    }

    for (auto _ : state){
        run_workload(db, workload);
    }
}

BENCHMARK_F(Fixture, sstable_read_from_ssfile_filter)(benchmark::State &state){
    std::unique_ptr<DbMemCache> memCache(new BST<std::string, DbValue>);
    SSTableDb db(std::move(memCache), sstableDirectory, true, true);
    auto workload = workloadGenerator->onlyInsertsWorkload(SSTable::maxMemcacheSize + 1);
    run_workload(db, workload);
    for (auto action : workload){
        action.operation = Operation::GET;
    }

    for (auto _ : state){
        run_workload(db, workload);
    }
}

BENCHMARK_MAIN();