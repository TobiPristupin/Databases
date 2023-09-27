#include <algorithm>
#include "AvlTree.h"

AvlTree::AvlTree() {
    root = nullptr;
}

void AvlTree::insert(const Entry &entry) {
    auto key = entry.key;
    if (!root) {
        root = std::make_unique<Node>(entry);
        return;
    }

    auto curr = root.get();
    while (curr) {
        if (key == curr->entry.key) {
            curr->entry.value = entry.value;
            return;
        } else if (key < curr->entry.key) {
            if (!curr->left) {
                curr->left = std::make_unique<Node>(entry);
                treeSize++;
                return;
            }
            curr = curr->left.get();
        } else if (key > curr->entry.key) {
            if (!curr->right) {
                curr->right = std::make_unique<Node>(entry);
                treeSize++;
                return;
            }
            curr = curr->right.get();
        }
    }
}
std::optional<DbValue> AvlTree::get(const std::string &key) {
    std::optional<Node *> node = findNode(key);
    if (node.has_value()) {
        return node.value()->entry.value;
    }

    return std::nullopt;
}

std::optional<AvlTree::Node *> AvlTree::findNode(const std::string &key) {
    auto curr = root.get();
    while (curr) {
        if (key == curr->entry.key) {
            return curr;
        } else if (key < curr->entry.key) {
            curr = curr->left.get();
        } else {
            curr = curr->right.get();
        }
    }

    return std::nullopt;
}
std::vector<Entry> AvlTree::toList() {
    std::vector<Entry> items;
    std::stack<Node *> stack;
    auto curr = root.get();
    while (curr || !stack.empty()) {
        if (curr) {
            stack.push(curr);
            curr = curr->left.get();
            continue;
        }

        curr = stack.top();
        stack.pop();
        items.push_back(curr->entry);
        curr = curr->right.get();
    }

    return items;
}

bool AvlTree::remove(const std::string &key) {
    return false;
}

void AvlTree::clear() {
    root.reset();
}

size_t AvlTree::size() const {
    return treeSize;
}
