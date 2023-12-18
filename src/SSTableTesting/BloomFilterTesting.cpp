#include <gtest/gtest.h>
#include "../DatabaseEntry.h"
#include "../SSTable/BST.hpp"
#include "../SSTable/SSFileCreator.h"
#include "../Workload.h"
#include "../SSTable/BloomFilter.h"

class BloomFilterTest : public testing::Test {
protected:
    BloomFilterTest() {
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

TEST_F(BloomFilterTest, testContainsInsertedKeys){
    auto keysToInclude = workloadGenerator->generateRandomKeyValues(500, 256);
    for (const auto& [key, value] : keysToInclude){
        memCache->insert(key, value);
    }

    BloomFilter filter(3, 20000, memCache.get(), {});
    for (const auto& [key, value] : keysToInclude){
        ASSERT_TRUE(filter.canContainKey(key));
    }
}

TEST_F(BloomFilterTest, testFalsePositiveRate){
    auto keysToInclude = workloadGenerator->generateRandomKeyValues(5000, 256);
    auto keysToExclude = workloadGenerator->generateRandomKeyValues(5000, 256);
    for (const auto& [key, value] : keysToInclude){
        memCache->insert(key, value);
    }

    BloomFilter filter(3, 20000, memCache.get(), {});
    int falsePositives = 0;
    for (const auto& [key, value] : keysToExclude){
        falsePositives += filter.canContainKey(key);
    }

    double errorRate = falsePositives / (double) keysToExclude.size();
    // Error rate depends on number of key values and bloom filter size. For the current params we expect ~15%
    double expectedErrorRate = 0.15;
    ASSERT_NEAR(errorRate, expectedErrorRate, 0.1);
}
