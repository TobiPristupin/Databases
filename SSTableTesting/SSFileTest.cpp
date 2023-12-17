#include <gtest/gtest.h>
#include "../DatabaseEntry.h"
#include "../SSTable/BST.hpp"
#include "../SSTable/SSFileCreator.h"
#include "../Workload.h"

class SSFileTest : public testing::Test {
protected:
    SSFileTest() {
        seed = time(nullptr);
        std::cout << "seed for reproducibility " << std::to_string(seed) << "\n";
        workloadGenerator = std::make_unique<WorkloadGenerator>(seed, SSTable::maxKeySize);
        initializeMemCache();
    }

    void SetUp() override {
        initializeMemCache();
    }

    void initializeMemCache(){
        memCache = std::make_unique<BST<std::string, DbValue>>();
    }

    std::unique_ptr<DbMemCache> memCache;
    std::unique_ptr<WorkloadGenerator> workloadGenerator;
    unsigned int seed;
    const std::string fileDirectory = "./";
};

TEST_F(SSFileTest, testFileIndex){
    for (int index = 0; index < 10; index++){
        auto ssFile = SSFileCreator::newFile(fileDirectory, index, 0, memCache.get(), {});
        ASSERT_EQ(ssFile->getIndex(), index);
    }
}

static void populate(const std::vector<Action> &workload, std::map<std::string, DbValue> &mirror, std::set<std::string> &tombstones, DbMemCache* memCache){
    for (auto &action: workload) {
        switch (action.operation) {
            case Operation::INSERT:
                mirror[action.key] = action.value;
                memCache->insert(action.key, action.value);
                tombstones.erase(action.key);
                break;
            case Operation::DELETE:
                memCache->remove(action.key);
                mirror.erase(action.key);
                tombstones.insert(action.key);
                break;
            case Operation::GET:
                break;
        }
    }
}

TEST_F(SSFileTest, testNoFilter) {
    std::map<std::string, DbValue> mirror;
    std::set<std::string> tombstones;
    auto workload = workloadGenerator->generateRandomWorkload(20000, 20);
    populate(workload, mirror, tombstones, memCache.get());
    auto ssFile = SSFileCreator::newFile(fileDirectory, 0, 0, memCache.get(), tombstones);
    for (const auto& [key, val] : mirror) {
        auto read = ssFile->get(key);
        ASSERT_EQ(read.type, KEY_FOUND);
        auto value = read.value.value();
        if (std::holds_alternative<double>(val)) {
            ASSERT_NEAR(std::get<double>(val), std::get<double>(value), 0.00001);
            continue;
        }

        ASSERT_EQ(val, value);
    }

    for (const auto& key : tombstones){
        ASSERT_EQ(ssFile->get(key).type, KEY_TOMBSTONE);
    }

    auto keyValuesNotInserted = workloadGenerator->generateRandomKeyValues(30, 256);
    for (const auto& [key, val] : keyValuesNotInserted){
        ASSERT_EQ(ssFile->get(key).type, KEY_NOT_FOUND);
    }
}

TEST_F(SSFileTest, testFilter) {
    std::map<std::string, DbValue> mirror;
    std::set<std::string> tombstones;
    auto workload = workloadGenerator->generateRandomWorkload(20000, 20);
    populate(workload, mirror, tombstones, memCache.get());
    auto ssFile = SSFileCreator::newFile(fileDirectory, 0, SSTable::bloomFilterBits, memCache.get(), tombstones);
    for (const auto& [key, val] : mirror) {
        auto read = ssFile->get(key);
        ASSERT_EQ(read.type, KEY_FOUND);
        auto value = read.value.value();
        if (std::holds_alternative<double>(val)) {
            ASSERT_NEAR(std::get<double>(val), std::get<double>(value), 0.00001);
            continue;
        }

        ASSERT_EQ(val, value);
    }

    for (const auto& key : tombstones){
        ASSERT_EQ(ssFile->get(key).type, KEY_TOMBSTONE);
    }

    auto keyValuesNotInserted = workloadGenerator->generateRandomKeyValues(30, 256);
    for (const auto& [key, val] : keyValuesNotInserted){
        ASSERT_EQ(ssFile->get(key).type, KEY_NOT_FOUND);
    }
}
