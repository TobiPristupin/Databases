#include <gtest/gtest.h>
#include "../SSTable/DatabaseEntry.h"
#include "../SSTable/AvlTree.hpp"
#include "../SSTable/Workload.h"

class MemcacheTest : public testing::Test {
protected:
    MemcacheTest() {
        seed = time(nullptr);
        std::cout << "seed for reproducibility " << std::to_string(seed) << "\n";
        workloadGenerator = std::make_unique<WorkloadGenerator>(seed);
        initializeMemCache();
    }

    void SetUp() override {
        initializeMemCache();
    }

    void initializeMemCache(){
        memCache = std::make_unique<AvlTree<std::string, DbValue>>();
    }

    std::unique_ptr<DbMemCache> memCache;
    std::unique_ptr<WorkloadGenerator> workloadGenerator;
    unsigned int seed;
};

TEST_F(MemcacheTest, testCorrectness){
    std::map<std::string, DbValue> mirror;
    auto workload = workloadGenerator->generateRandomWorkload(5000, 20);
    for (auto &action : workload){
        ASSERT_EQ(mirror.size(), memCache->size());
        switch (action.operation) {
            case Operation::GET:
                if (mirror.find(action.key) != mirror.end()){
                    ASSERT_EQ(mirror.at(action.key), memCache->get(action.key).value());
                } else {
                    ASSERT_FALSE(memCache->get(action.key).has_value());
                }
                break;
            case Operation::INSERT:
                mirror[action.key] = action.value;
                memCache->insert(action.key, action.value);
                break;
            case Operation::DELETE:
                ASSERT_EQ(mirror.erase(action.key) >= 1, memCache->remove(action.key));
                break;
            case Operation::ENUM_COUNT:
                break;
        }
    }
}

TEST_F(MemcacheTest, testTraverseSorted){
    std::map<std::string, DbValue> mirror;
    auto workload = workloadGenerator->generateRandomWorkload(5000, 20);
    for (auto &action : workload){
        switch (action.operation) {
            case Operation::INSERT:
                mirror[action.key] = action.value;
                memCache->insert(action.key, action.value);
                break;
            case Operation::GET:
            case Operation::DELETE:
            case Operation::ENUM_COUNT:
                break;
        }
    }

    auto it = mirror.begin();
    memCache->traverseSorted([this, &it](auto key, auto value) {
        ASSERT_EQ(it->first, key);
        ASSERT_EQ(it->second, value);
        ++it;
    });
}
