#include <memory>
#include <iostream>
#include <cstring>
#include <fstream>
#include <regex>
#include <variant>
#include <random>


int main() {
    const char charset[] =
            "012";
    std::cout << charset[sizeof(charset) - 1] << "\n";
}