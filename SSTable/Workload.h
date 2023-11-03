#ifndef DATAINTENSIVE_WORKLOAD_H
#define DATAINTENSIVE_WORKLOAD_H

#include <string>
#include <random>
#include <map>
#include "DatabaseEntry.h"

enum class Operation {
    GET,
    INSERT,
    DELETE,
    ENUM_COUNT
};

struct Action {

    Action(Operation operation, std::string key, DbValue value);

    Operation operation;
    std::string key;
    DbValue value;
};

class WorkloadGenerator {

public:
    explicit WorkloadGenerator(unsigned long seed);
    /*
     * Example: If numActions = 1000 and expectedActionsPerKeyValue = 10, then that means that the
     * workload will generate 100 unique key-value pairs, and sample one at random 1000 times. That means
     * that the expected actions in the workload that use a singular unique key-value pair is ~10.
     */
    std::vector<Action> generateRandomWorkload(size_t numActions, size_t expectedActionsPerKeyValue);

private:
    std::default_random_engine randomEngine;
    Action generateRandomAction(const std::vector<std::pair<std::string, DbValue>> &keyValuePairs, std::map<std::string, bool> &inserted);
    std::vector<std::pair<std::string, DbValue>> generateRandomKeyValues(size_t numPairs, size_t maxKeyLength);
    std::string randomString(size_t length);
    DbValue randomDbValue();
    Operation randomOperation();
    int randomBetween(int min, int max);
    double randomBetween(double min, double max);
};

#endif
