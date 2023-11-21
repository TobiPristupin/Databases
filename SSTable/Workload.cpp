#include "Workload.h"

#include <utility>
#include <map>
#include <random>
#include <stdexcept>
#include <iostream>

Action::Action(Operation operation, std::string key, DbValue value) : operation(operation), key(std::move(key)), value(std::move(value)) {}

WorkloadGenerator::WorkloadGenerator(unsigned long seed) : randomEngine(seed) {}

std::vector<Action> WorkloadGenerator::generateRandomWorkload(size_t numActions, size_t expectedActionsPerKeyValue) {
    size_t numKeyValuePairs = numActions / expectedActionsPerKeyValue;
    std::vector<Action> actions;
    actions.reserve(numActions);
    auto keyValues = generateRandomKeyValues(numKeyValuePairs, maxKeySize);
    while (actions.size() < numActions){
        actions.push_back(generateRandomAction(keyValues));
    }

    return actions;
}

std::vector<Action>
WorkloadGenerator::generateCorrectRandomWorkload(size_t numActions, size_t expectedActionsPerKeyValue) {
    size_t numKeyValuePairs = numActions / expectedActionsPerKeyValue;
    std::vector<Action> actions;
    actions.reserve(numActions);
    auto keyValues = generateRandomKeyValues(numKeyValuePairs, maxKeySize);
    std::map<std::string, bool> inserted;

    while (actions.size() < numActions){
        actions.push_back(generateCorrectRandomAction(keyValues, inserted));
    }

    return actions;
}

Action WorkloadGenerator::generateRandomAction(const std::vector<std::pair<std::string, DbValue>> &keyValuePairs) {
    std::pair<std::string, DbValue> keyValuePair;
    std::sample(keyValuePairs.begin(), keyValuePairs.end(), &keyValuePair, 1, randomEngine);
    auto operation = randomOperation();
    return {operation, keyValuePair.first, keyValuePair.second};
}

Action WorkloadGenerator::generateCorrectRandomAction(const std::vector<std::pair<std::string, DbValue>> &keyValuePairs,
                                                      std::map<std::string, bool> &inserted) {
    std::pair<std::string, DbValue> keyValuePair;
    std::sample(keyValuePairs.begin(), keyValuePairs.end(), &keyValuePair, 1, randomEngine);
    auto operation = randomOperation();

    while (!validOperation(operation, inserted[keyValuePair.first])){
        operation = randomOperation();
    }

    if (operation == Operation::INSERT){
        inserted[keyValuePair.first] = true;
    } else if (operation == Operation::DELETE){
        inserted[keyValuePair.first] = false;
    }

    return {operation, keyValuePair.first, keyValuePair.second};
}

std::vector<std::pair<std::string, DbValue>>
WorkloadGenerator::generateRandomKeyValues(size_t numPairs, size_t maxKeyLength) {
    std::vector<std::pair<std::string, DbValue>> pairs;
    while (numPairs--){
        pairs.emplace_back(randomString(randomBetween(1, static_cast<int>(maxKeyLength))), randomDbValue());
    }

    return pairs;
}

std::string WorkloadGenerator::randomString(size_t length) {
    auto randChar = [this, &length]() -> char
    {
        const char charset[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
        return charset[randomBetween(1, sizeof(charset)-2)];
    };

    std::string str(length,0);
    std::generate_n( str.begin(), length, randChar );
    return str;
}

DbValue WorkloadGenerator::randomDbValue() {
    int valueTypes = std::variant_size_v<DbValue>;
    auto typeIndex = randomBetween(0, valueTypes-1);
    switch (typeIndex) {
        case intType:
            return randomBetween(-1000, 1000);
        case longType:
            return static_cast<long>(randomBetween(-1000, 1000));
        case doubleType:
            return randomBetween(-1000.0, 1000.0);
        case boolType:
            return static_cast<bool>(randomBetween(0, 1));
        case stringType:
            return randomString(randomBetween(1, 50));
        default:
            throw std::runtime_error("Unexpected type index " + std::to_string(typeIndex));
    }
}

Operation WorkloadGenerator::randomOperation() {
    return Operation(randomBetween(0, static_cast<int>(Operation::ENUM_COUNT)-1));
}

int WorkloadGenerator::randomBetween(int min, int max) {
    std::uniform_int_distribution<int> uniform(min, max);
    return uniform(randomEngine);
}

double WorkloadGenerator::randomBetween(double min, double max) {
    std::uniform_real_distribution<double> uniform(min, max);
    return uniform(randomEngine);
}

bool WorkloadGenerator::validOperation(const Operation &operation, bool keyInserted) {
    if (!keyInserted){
        if (operation == Operation::GET || operation == Operation::DELETE){
            return false;
        }
    }

    return true;
}



