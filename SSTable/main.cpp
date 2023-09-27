#include <iostream>
#include "AvlTree.h"

int main(){
    AvlTree<int> tree;
    tree.insert("1", 1);
    tree.insert("2", 2);
    tree.insert("3", 3);
    tree.insert("6", 6);
    tree.insert("0", 0);
    tree.insert("5", 5);
    for (auto const &x : tree.toList()){
        std::cout << x.value << "\n";
    }
    return 0;
}