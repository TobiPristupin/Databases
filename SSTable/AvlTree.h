#ifndef DATAINTENSIVE_AVLTREE_H
#define DATAINTENSIVE_AVLTREE_H

#include <functional>
#include "MemCache.h"

class AvlTree : public MemCache {
public:

    AvlTree();
    void insert(const Entry &entry) override;
    std::optional<DbValue> get(const std::string &key) const override;
    bool remove(const std::string& key) override;
    void traverseSorted(const std::function<void(const Entry&)>& callback) const override;
    size_t size() const override;
    void clear() override;

    struct Node {
        explicit Node(Entry  entry, Node* left=nullptr, Node *right=nullptr) : entry(std::move(entry)), left(left), right(right) {};
        Entry entry;
        std::unique_ptr<Node> left, right;
    };

private:

    size_t treeSize;
    std::unique_ptr<Node> root;
    void traverseInorder(const std::function<void(const Entry&)>& callback, Node* root) const;
    std::optional<Node*> findNode(const std::string &key) const;
    std::unique_ptr<Node> removeRecursive(std::unique_ptr<Node> &&curr, const std::string &key);
    Node* findSuccessor(Node* nOde);
};



#endif //DATAINTENSIVE_AVLTREE_H
