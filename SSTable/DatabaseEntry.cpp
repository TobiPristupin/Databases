#include <stdexcept>
#include <cstring>
#include "DatabaseEntry.h"

std::string dbValueToString(const DbValue& value) {
    if (const int *v = std::get_if<int>(&value)){
        return std::to_string(*v);
    } else if (const long *v = std::get_if<long>(&value)){
        return std::to_string(*v);
    } else if (const double *v = std::get_if<double>(&value)){
        return std::to_string(*v);
    } else if (const bool *v = std::get_if<bool>(&value)){
        return *v ? "true" : "false";
    } else if (const std::string *v = std::get_if<std::string>(&value)){
        return *v;
    }

    throw std::runtime_error("Unrecognized type index" + std::to_string(value.index()));
}

DbValue dbValueFromString(size_t typeIndex, const std::string &value) {
    DbValue dbValue;
    switch (typeIndex) {
        case intType: {
            dbValue = std::stoi(value);
            break;
        }
        case longType: {
            dbValue = std::stol(value);
            break;
        }
        case doubleType: {
            dbValue = std::stod(value);
            break;
        }
        case boolType: {
            if (value != "true" && value != "false"){
                throw std::runtime_error("Expected boolean type to have value 'true' or 'false'. Instead, it had value " + value);
            }
            dbValue = (value == "true");
            break;
        }
        case stringType: {
            dbValue = value;
            break;
        }
        default: throw std::runtime_error("Invalid type index " + std::to_string(typeIndex));
    }

    return dbValue;
}
