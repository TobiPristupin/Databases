#include <stdexcept>
#include "DatabaseEntry.h"

Entry::Entry(std::string key, DbValue value) : key(std::move(key)), value(std::move(value)) {}


std::string Entry::valueToString() const {
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
}

Entry::Entry(std::string key, int type_index, const std::string &value) : key(std::move(key)) {
    switch (type_index) {
        case 0: {
            this->value = std::stoi(value);
            break;
        }
        case 1: {
            this->value = std::stol(value);
            break;
        }
        case 2: {
            this->value = std::stod(value);
            break;
        }
        case 3: {
            if (value != "true" && value != "false"){
                throw std::runtime_error("Expected boolean type to have value 'true' or 'false'. Instead, it had value " + value);
            }
            this->value = (value == "true");
            break;
        }
        case 4: {
            this->value = value;
            break;
        }
        default: throw std::runtime_error("Invalid type index " + std::to_string(type_index));
    }
}
