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
std::optional<DbValue> AvlTree::get(const std::string &key) const {
    std::optional<Node *> node = findNode(key);
    if (node.has_value()) {
        return node.value()->entry.value;
    }

    return std::nullopt;
}

std::optional<AvlTree::Node *> AvlTree::findNode(const std::string &key) const {
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

bool AvlTree::remove(const std::string &key) {
    if (!root) {
        return false;
    }

    auto curr = &root;
    auto prev = curr;
    while (curr) {
        if (curr->get()->entry.key < key) {
            prev = curr;
            curr = &curr->get()->left;
        } else if (curr->get()->entry.key > key) {
            prev = curr;
            curr = &curr->get()->right;
        } else {
            break;
        }
    }

    if (!curr) {
        return false;
    }

    if (!curr->get()->left && !curr->get()->right) {

    }

}

//Node* &AvlTree::findSuccessor(const std::unique_ptr<Node> &node) {
//    if (!node->right){
//        throw std::runtime_error("Expected node to have right subtree when searching for successor");
//    }
//
//    auto curr = &node->right;
//    while (curr->get()->left){
//        curr = &curr->get()->left;
//    }
//
//    return curr;
//}

void AvlTree::traverseSorted(const std::function<void(const Entry &)>& callback) const {
    traverseInorder(callback, root.get());
}

void AvlTree::traverseInorder(const std::function<void(const Entry &)>& callback, AvlTree::Node *node) const {
    if (!node){
        return;
    }

    traverseInorder(callback, node->left.get());
    callback(node->entry);
    traverseInorder(callback, node->right.get());
}

void AvlTree::clear() {
    root.reset();
}

size_t AvlTree::size() const {
    return treeSize;
}


