#include <gtest/gtest.h>
#include "../SSTable/DatabaseEntry.h"
#include "../SSTable/AvlTree.hpp"

class MemcacheTest : public testing::Test {
protected:
    void SetUp() override {
        memCache = std::make_unique<AvlTree<std::string, DbValue>>();
    }

    std::unique_ptr<DbMemCache> memCache;
    const unsigned int seed = 1;
};

TEST(MemcacheTest, test1){
    std::map<std::string, DbValue> mirror;


}

//std::vector<std::pair<std::string, DbValue>>