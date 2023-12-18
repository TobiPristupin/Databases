#ifndef DATAINTENSIVE_BST_HPP
#define DATAINTENSIVE_BST_HPP

#include <functional>
#include "MemCache.h"

template<class K, class V>
class BST : public MemCache<K, V> {
public:

    BST();
    std::optional<V> get(const K& k) const override;
    void insert(const K& k, const V& v) override;
    bool remove(const K& key) override;
    void traverseSorted(const std::function<void(const K& k, const V& v)>& callback) const override;
    size_t size() const override;
    void clear() override;
    ~BST() override;

    struct Node {
        explicit Node(K k, V v, Node* left=nullptr, Node *right=nullptr) : key(std::move(k)), value(std::move(v)), left(left), right(right) {};
        K key;
        V value;
        Node *left, *right;
    };

private:

    size_t treeSize;
    std::optional<Node*> root;
    Node* removeRecursive(Node* curr, const K& key);
    std::optional<Node*> findNode(const K& k) const;
    Node* findSuccessor(Node* node) const;
    void clearRecursive(Node* curr);
};

template<class K, class V>
BST<K, V>::BST() : treeSize(0), root(std::nullopt) {}

template<class K, class V>
std::optional<V> BST<K, V>::get(const K &k) const {
    auto node = findNode(k);
    if (node.has_value()){
        return node.value()->value;
    }

    return std::nullopt;
}

template<class K, class V>
void BST<K, V>::insert(const K &k, const V &v) {
    if (!root.has_value()){
        root = new Node(k, v);
        treeSize++;
        return;
    }

    auto curr = root.value();
    while (curr){
        if (k == curr->key){
            curr->value = v;
            return;
        } else if (k < curr->key){
            if (!curr->left){
                curr->left = new Node(k, v);
                treeSize++;
                return;
            }
            curr = curr->left;
        } else {
            if (!curr->right){
                curr->right = new Node(k, v);
                treeSize++;
                return;
            }
            curr = curr->right;
        }
    }

    throw std::runtime_error("Logic error when inserting.");
}

template<class K, class V>
bool BST<K, V>::remove(const K &key) {
    auto node = findNode(key);
    if (!node.has_value()){
        return false;
    }

    auto newRoot = removeRecursive(root.value(), key);
    treeSize--;
    if (newRoot){
        root = newRoot;
    } else {
        root = std::nullopt;
    }

    return true;
}


template<class K, class V>
void BST<K, V>::traverseSorted(const std::function<void(const K &, const V &)> &callback) const {
    if (!root.has_value()){
        return;
    }

    std::stack<Node*> nodes;
    auto curr = root.value();
    while (curr || !nodes.empty()){
       while (curr){
           nodes.push(curr);
           curr = curr->left;
       }

       curr = nodes.top();
       nodes.pop();
       callback(curr->key, curr->value);
       curr = curr->right;
    }
}

template<class K, class V>
size_t BST<K, V>::size() const {
    return treeSize;
}

template<class K, class V>
typename BST<K,V>::Node *BST<K, V>::removeRecursive(BST::Node *curr, const K &key) {
    if (curr->key < key){
        curr->right = removeRecursive(curr->right, key);
        return curr;
    } else if (curr->key > key){
        curr->left = removeRecursive(curr->left, key);
        return curr;
    }


    if (!curr->left){
        auto right = curr->right;
        delete curr;
        return right;
    } else if (!curr->right){
        auto left = curr->left;
        delete curr;
        return left;
    }

    auto successor = findSuccessor(curr);
    curr->key = successor->key;
    curr->value = successor->value;
    successor->key = key;
    curr->right = removeRecursive(curr->right, key);
    return curr;
}

template<class K, class V>
void BST<K, V>::clear() { //TODO
    if (!root.has_value()){
        return;
    }

    treeSize = 0;
    return clearRecursive(root.value());
}

template<class K, class V>
std::optional<typename BST<K,V>::Node*> BST<K, V>::findNode(const K &k) const {
    if (!root.has_value()){
        return std::nullopt;
    }

    auto curr = root.value();
    while (curr){
        if (k == curr->key){
            return curr;
        } else if (k < curr->key){
            curr = curr->left;
        } else {
            curr = curr->right;
        }
    }

    return std::nullopt;
}

template<class K, class V>
typename BST<K,V>::Node *BST<K, V>::findSuccessor(BST::Node *node) const {
    if (!node->right){
        throw std::runtime_error("Expected node to have right subtree");
    }

    auto curr = node->right;
    while (curr->left){
        curr = curr->left;
    }

    return curr;
}


template<class K, class V>
BST<K, V>::~BST() {
    clear();
}

template<class K, class V>
void BST<K, V>::clearRecursive(BST::Node *curr) { //TODO: Make iterative
    if (!curr){
        return;
    }

    clearRecursive(curr->left);
    clearRecursive(curr->right);
    if (root.has_value() && root.value() == curr){
        root = std::nullopt;
    }
    delete curr;
}

#endif //DATAINTENSIVE_BST_HPP
