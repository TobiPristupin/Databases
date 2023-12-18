#include <gtest/gtest.h>
#include "../LogDatabase/LogDatabase.h"
#include "../SSTable/BST.hpp"
#include "../Workload.h"

class LogDatabaseTest : public testing::Test {
protected:
    LogDatabaseTest() {
        seed = time(nullptr);
        std::cout << "seed for reproducibility " << std::to_string(seed) << std::endl;
        workloadGenerator = std::make_unique<WorkloadGenerator>(seed);
    }

    void SetUp() override {}

    std::unique_ptr<WorkloadGenerator> workloadGenerator;
    unsigned int seed;
};

void handleGet(const Action &action, const std::map<std::string, DbValue> &mirror, LogDatabase &database);

TEST_F(LogDatabaseTest, testCorrectness){
    std::map<std::string, DbValue> mirror;
    auto database = LogDatabase(true);
    auto workload = workloadGenerator->generateRandomWorkload(5000, 20);
    for (auto &action : workload){
        switch (action.operation) {
            case Operation::GET:
                handleGet(action, mirror, database);
                break;
            case Operation::INSERT:
                mirror[action.key] = action.value;
                database.insert(action.key, action.value);
                break;
            case Operation::DELETE:
                ASSERT_EQ(mirror.erase(action.key) >= 1, database.remove(action.key));
                break;
        }
    }
}

void handleGet(const Action &action, const std::map<std::string, DbValue> &mirror, LogDatabase &database){
    if (mirror.find(action.key) != mirror.end()){
        if (std::holds_alternative<double>(action.value)) {
            ASSERT_NEAR(std::get<double>(action.value), std::get<double>(database.get(action.key).value()), 0.00001);
        } else {
            ASSERT_EQ(mirror.at(action.key), database.get(action.key).value());
        }
    } else {
        ASSERT_FALSE(database.get(action.key).has_value());
    }
}