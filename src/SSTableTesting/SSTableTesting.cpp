#include <gtest/gtest.h>
#include "../DatabaseEntry.h"
#include "../SSTable/BST.hpp"
#include "../SSTable/SSFileCreator.h"
#include "../Workload.h"
#include "../SSTable/BloomFilter.h"
#include "../SSTable/SSTableDb.h"

class SSTableTest : public testing::Test {
protected:
    SSTableTest() {
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
};

TEST_F(SSTableTest, testNoBloomFilter){
    SSTableDb ssTableDb(std::move(memCache), "/home/pristu/Documents/School/DataIntensive/src/SSTable", true, false);
    std::map<std::string, DbValue> mirror;
    auto workload = workloadGenerator->generateRandomWorkload(50000, 5);
    for (auto &action : workload){
        switch (action.operation) {
            case Operation::GET:
                if (mirror.find(action.key) != mirror.end()){
                    if (std::holds_alternative<double>(action.value)) {
                        ASSERT_NEAR(std::get<double>(mirror.at(action.key)), std::get<double>(ssTableDb.get(action.key).value()), 0.0001);
                    } else {
                        ASSERT_EQ(mirror.at(action.key), ssTableDb.get(action.key).value());
                    }
                } else {
                    ASSERT_FALSE(ssTableDb.get(action.key).has_value());
                }
                break;
            case Operation::INSERT:
                mirror[action.key] = action.value;
                ssTableDb.insert(action.key, action.value);
                break;
            case Operation::DELETE:
                mirror.erase(action.key);
                ssTableDb.remove(action.key);
                break;
        }
    }
}

TEST_F(SSTableTest, testBloomFilter){
    SSTableDb ssTableDb(std::move(memCache), "/home/pristu/Documents/School/DataIntensive/src/SSTable", true, true);
    std::map<std::string, DbValue> mirror;
    auto workload = workloadGenerator->generateRandomWorkload(50000, 5);
    for (auto &action : workload){
        switch (action.operation) {
            case Operation::GET:
                if (mirror.find(action.key) != mirror.end()){
                    if (std::holds_alternative<double>(action.value)) {
                        ASSERT_NEAR(std::get<double>(mirror.at(action.key)), std::get<double>(ssTableDb.get(action.key).value()), 0.0001);
                    } else {
                        ASSERT_EQ(mirror.at(action.key), ssTableDb.get(action.key).value());
                    }
                } else {
                    ASSERT_FALSE(ssTableDb.get(action.key).has_value());
                }
                break;
            case Operation::INSERT:
                mirror[action.key] = action.value;
                ssTableDb.insert(action.key, action.value);
                break;
            case Operation::DELETE:
                mirror.erase(action.key);
                ssTableDb.remove(action.key);
                break;
        }
    }
}