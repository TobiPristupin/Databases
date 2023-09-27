#ifndef DATAINTENSIVE_AVLTREE_H
#define DATAINTENSIVE_AVLTREE_H

#include <string>
#include <optional>
#include <utility>
#include <vector>
#include <memory>
#include <stack>
#include "DatabaseEntry.h"

class AvlTree {
public:

    AvlTree();
    void insert(const Entry &entry);
    std::optional<DbValue> get(const std::string &key);
    bool remove(const std::string& key);
    std::vector<Entry> toList();
    size_t size() const;
    void clear();


private:
    struct Node {
        explicit Node(Entry  entry, Node* left=nullptr, Node *right=nullptr) : entry(std::move(entry)), left(left), right(right) {};
        Entry entry;
        std::unique_ptr<Node> left, right;
    };

    size_t treeSize;
    std::unique_ptr<Node> root;
    std::optional<Node*> findNode(const std::string &key);
};


//    auto curr = &root;
//    if (root && root.get()->entry.key == key){
//        root.reset();
//    }
//
//    auto prev = curr;
//    while (curr){
//        if (curr->entry.key < key){
//            prev = curr;
//            curr = curr->left;
//            continue;
//        } else if (curr->entry.key > key){
//            prev = curr;
//            curr = curr->right;
//            continue;
//        }
//
//       bool isLeftChild = curr->entry.value < prev->entry.value;
//       if (!curr->left && !curr->right){
//           if (isLeftChild){
//               prev->left = nullptr;
//           } else {
//               prev->right = nullptr;
//           }
//       }
//
//       curr.reset();
//    }
//
//
//    return false;




#endif
