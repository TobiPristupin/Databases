#include <iostream>
#include "LogDatabase.h"

int main() {
//    LogDatabase database(true);
//    database.set<int>("key_1", 1);
//    database.set<std::string>("keyy_2", "putoloco");
//    database.set("key_3", 1234);
    LogDatabase database;
    std::cout << database.get<int>("key_3") << "\n";
    std::cout << database.get<int>("key_1") << "\n";
    std::cout << database.get<std::string>("keyy_2") << "\n";
    return 0;
}
