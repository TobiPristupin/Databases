#include <gtest/gtest.h>
#include "../SSTable/DatabaseEntry.h"
#include "../SSTable/AvlTree.hpp"
#include "../SSTable/SSFileCreator.h"
#include "../SSTable/SSFile.h"
#include "../SSTable/Workload.h"

class SSFileTest : public testing::Test {
protected:
    SSFileTest() {
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
    const std::string fileDirectory = "./";
};

TEST_F(SSFileTest, testCorrectness) {
    std::map<std::string, DbValue> mirror;
    std::unordered_set<std::string> tombstones;
    auto workload = workloadGenerator->generateRandomWorkload(5000, 20);
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
            case Operation::ENUM_COUNT:
                break;
        }
    }

    auto ssFile = SSFileCreator::newFile(fileDirectory, 0, memCache.get(), tombstones);
    for (auto const &keyVal: mirror) {
        if (std::holds_alternative<double>(keyVal.second)) {
            ASSERT_NEAR(std::get<double>(keyVal.second), std::get<double>(ssFile->get(keyVal.first).value()), 0.00001);
            continue;
        }
        ASSERT_EQ(keyVal.second, ssFile->get(keyVal.first).value());
    }
}