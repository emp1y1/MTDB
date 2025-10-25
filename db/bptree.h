

#ifndef LEVELDB_BPTREE_H
#define LEVELDB_BPTREE_H
#include "dbformat.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>
#include <mutex>
#include "leveldb/iterator.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "util/arena.h"
#include "util/coding.h"
#include "db_impl.h"
#include "ValidKeyVector.h"

using namespace std;
using namespace std::chrono;

namespace leveldb {
// 静态成员变量用于记录B+树的总内存使用量
//static int cou;
static int totalMemoryUsage=0; // 静态成员变量用于记录B+树的真实键值对的内存使用量
static int hotkeyindexMemoryUsage=0; // 静态成员变量用于记录B+树的总内存使用量
static uint64_t average_optime=0;
static uint8_t range_num=0;
//static std::vector<std::pair<uint32_t , uint32_t>> validkey;
//string fs_key=""; //last flush stop key;
static uint8_t random_access_count=0;
//extern leveldb::ValidKeyVector* ValidKey;

//vector<flushKeyValue> flushvector;
class Node {

 public:
  ~Node() {

    children.clear();
    nodeInfo.clear();
  }

  Node(Node *parent = nullptr, bool isLeaf = false, Node *prev_ = nullptr,
       Node *next_ = nullptr)
      : parent(parent), isLeaf(isLeaf), prev(prev_), next(next_) {

    if (next_) {
      next_->prev = this;
    }

    if (prev_) {
      prev_->next = this;
    }

  }
  struct NodeInfo{
    string key;
    string value;
    bool true_value;
    uint32_t  last_optime;
    uint8_t internalkeytype;//is tomb
    uint64_t internalkeysequence;
    uint16_t sst_num;
    uint32_t offset;
    uint16_t blocksize;
    uint8_t range_num_Node;
    NodeInfo(const string& _value): value(_value) {}
    ~NodeInfo() {}
  };
  using NodePtr = std::shared_ptr<Node>;
  vector<string> keys;
  Node *parent;

  vector<Node *> children;
  vector<NodeInfo*> nodeInfo;
  Node *next;
  Node *prev;

  bool isLeaf;



  int indexOfChild(const string &key) {
    for (int i = 0; i < keys.size(); i++) {
      if (key < keys[i]) {
        return i;
      }
    }
    return keys.size();
  }

  int indexOfKey(const string &key) {
    for (int i = 0; i < keys.size(); i++) {
      if (key == keys[i]) {
        return i;
      }
    }
    return -1;
  }

  Node *getChild(const string &key) { assert(indexOfChild(key) < children.size());return children[indexOfChild(key)]; }

  void setChild(const string &key, vector<Node *> value) {
    int i = indexOfChild(key);
    keys.insert(keys.begin() + i, key);
    children.erase(children.begin() + i);
    children.insert(children.begin() + i, value.begin(), value.end());
  }

  tuple<string, Node*, Node*> splitInternal() {
    Node* left = new Node(parent, false, nullptr, nullptr);
    int mid = keys.size() / 2;
    copy(keys.begin(), keys.begin() + mid, back_inserter(left->keys));
    copy(children.begin(), children.begin() + mid + 1, back_inserter(left->children));
    for (Node* child : left->children) {
      child->parent = left; // set parent of children to point to new left node
    }
    string key = keys[mid];
    keys.erase(keys.begin(), keys.begin() + mid + 1);
    children.erase(children.begin(), children.begin() + mid + 1);
    return make_tuple(key, left, this);
  }

  NodeInfo* get(const string &key,Status* s) {
    NodeInfo* tmpinfo;
    int index = -1;
    for (int i = 0; i < keys.size(); ++i) {
      if (keys[i].compare(key)==0 ) {
        index = i;
        //cout<<"find in bptree line117 needkey:"<<key<<endl;
        break;
      }
    }

    if (index == -1) {
      //cout<<" no find in bptree line117 "<<endl;
      *s = Status::NotFound(Slice());
      return tmpinfo;
    }
    auto now = std::chrono::system_clock::now();
    nodeInfo[index]->last_optime=std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    tmpinfo=nodeInfo[index];
    return tmpinfo;

  }
  void rangescan(string minkey,string maxkey) {
    int index = -1;
    if(keys[0].compare(minkey)>=0){
      index = 0;
    }else {
      for (int i = 1; i < keys.size(); ++i) {
        if (keys[i].compare(minkey) == 0 && keys[i - 1].compare(minkey) <= 0) {
          index = i;
          break;
        }
      }
    }
    if (index == -1) {
      cout << "minkey "  << " not found in this bptree" << endl;
    }//找到第一个叶子节点后，读取其中的数据
    while(index<keys.size() && keys[index] <= maxkey){
      cout<<keys[index]<<" ";
      index++;
    }//next继续向后找相邻叶子节点
    Node *tmpnode=next;
    while(tmpnode && tmpnode->keys[0] <= maxkey){
      index=0;
      while(index<tmpnode->keys.size() && tmpnode->keys[index] <= maxkey){
        cout<<tmpnode->keys[index]<<" ";
        index++;
      }
      index=0;
      tmpnode=tmpnode->next;
    }

  }
  void load_set(const string &key, const NodeInfo* tmpnodeInfo) {
    // cout<<cou++<<endl;
    int i = indexOfChild(key);
    if (find(keys.begin(), keys.end(), key) == keys.end()) {
      keys.insert(keys.begin() + i, key);
      nodeInfo.insert(nodeInfo.begin() + i,new NodeInfo(*tmpnodeInfo));
      if(tmpnodeInfo->true_value) {
        totalMemoryUsage +=
            key.size() + tmpnodeInfo->value.size() + 6;
      }else{
        hotkeyindexMemoryUsage += key.size() + 16 + 6;//插入新KV
  }
    } else {
      if(nodeInfo[i - 1]->true_value) {
        totalMemoryUsage = totalMemoryUsage +  tmpnodeInfo->value.size() -
                           nodeInfo[i - 1]->value.size();  //更新KV
        cout<<"B+tree UpdateKV"<<endl;
    }else {
        hotkeyindexMemoryUsage -= key.size() + 16 + 6;
        totalMemoryUsage = totalMemoryUsage + key.size()+ tmpnodeInfo->value.size()+6 ;
        cout<<"hotkeyindex UpdateKV"<<endl;
    }
      delete nodeInfo[i - 1];
      nodeInfo[i - 1] = new NodeInfo(*tmpnodeInfo);
      }


    // cout<<"insert key success lin191:"<<key<<endl;
    }
  void normal_set(const string &key, const string &value,uint8_t& internalkeytype,uint64_t& internalkeysequence) {
    // cout<<cou++<<endl;
    NodeInfo tmpInfo("");
    int i = indexOfChild(key);
    auto now = std::chrono::system_clock::now();
    tmpInfo.value=value;
    tmpInfo.true_value=true;
    tmpInfo.internalkeytype=internalkeytype;
    tmpInfo.internalkeysequence=internalkeysequence;
    tmpInfo.last_optime=std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    //cout<<key<<"   "<<value<<endl;
    //cout<<key.size()<<endl;
    if (find(keys.begin(), keys.end(), key) == keys.end()) {
      keys.insert(keys.begin() + i, key);
      nodeInfo.insert(nodeInfo.begin() + i,new NodeInfo(tmpInfo));
      totalMemoryUsage+=key.size()+value.size()+6;//插入新KV
    } else {
      if(nodeInfo[i - 1]->true_value) {
        totalMemoryUsage = totalMemoryUsage + value.size() -
                           nodeInfo[i - 1]->value.size();  //更新KV
        cout<<"B+tree UpdateKV"<<endl;
      }else{
        hotkeyindexMemoryUsage -= key.size() + 16 + 6;
        totalMemoryUsage = totalMemoryUsage + key.size()+value.size()+6 ;
        //validkey[nodeInfo[i - 1]->range_num_Node].second--;
        //ValidKey->my_structs_[nodeInfo[i - 1]->range_num_Node].valid_num--;//hotkey被更新，有效个数--
        cout<<"hotkeyindex UpdateKV"<<endl;
      }
      delete nodeInfo[i - 1];
      nodeInfo[i - 1] = new NodeInfo(tmpInfo);
    }


    //cout<<"insert key success lin191:"<<key<<endl;
  }
  void hotkey_set(const string &key, uint64_t &sst_num, uint64_t &offset,uint8_t &internalkeytype,uint64_t& internalkeysequence,uint64_t &blocksize) {
    // cout<<cou++<<endl;
    NodeInfo tmpInfo("");
    int i = indexOfChild(key);
    auto now = std::chrono::system_clock::now();
    tmpInfo.true_value=false;
    tmpInfo.sst_num=sst_num;
    tmpInfo.offset=offset;
    tmpInfo.internalkeytype=internalkeytype;
    tmpInfo.internalkeysequence=internalkeysequence;
    tmpInfo.blocksize=blocksize;
    tmpInfo.last_optime=std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    tmpInfo.range_num_Node=range_num;
    //cout<<key.size()<<endl;
    if (find(keys.begin(), keys.end(), key) == keys.end()) {
      keys.insert(keys.begin() + i, key);
      nodeInfo.insert(nodeInfo.begin() + i,new NodeInfo(tmpInfo));
      //cout<<"true_value "<<nodeInfo[i - 1]->true_value<<endl;
      //cout<<"sst_num "<<nodeInfo[i - 1]->sst_num<<endl;
      hotkeyindexMemoryUsage += key.size() + 16 + 6;//插入新KV
      //ValidKey->my_structs_[range_num].valid_num++;
    } else if(!nodeInfo[i - 1]->true_value) {
      //改totalMemoryUsage=totalMemoryUsage+value.size()-nodeInfo[i - 1]->value.size();//更新KV
      delete nodeInfo[i - 1];
      nodeInfo[i - 1] = new NodeInfo(tmpInfo);
      //ValidKey->my_structs_[range_num].valid_num++;
    }


    //cout<<"insert key success lin377:"<<key<<endl;
  }

  tuple<string, Node *, Node *> splitLeaf() {
    Node *left = new Node(parent, true, prev, this);
    int mid = keys.size() / 2;
    left->keys = vector<string>(keys.begin(), keys.begin() + mid);
    left->nodeInfo = vector<NodeInfo*>(nodeInfo.begin(), nodeInfo.begin() + mid);
    keys.erase(keys.begin(), keys.begin() + mid);
    nodeInfo.erase(nodeInfo.begin(), nodeInfo.begin() + mid);
    return make_tuple(keys[0], left, this);
  }

};


class BPlusTree {
 public:
  ~BPlusTree() {
    if (root) {
      destroyTree(root);
      root = nullptr;
    }
  }

  void destroyTree(Node* node) {
    if (node == nullptr) {
      return;
    }

    for (auto child : node->children) {
      destroyTree(child);
      child = nullptr;
    }

    for (auto nodeinfo : node->nodeInfo) {
      delete nodeinfo;
      nodeinfo = nullptr;
    }

    delete node;
    node = nullptr;
  }

  //std::shared_ptr<std::mutex> GetMutex() const { return mtx; }
  void IncrementActiveIterators() {
    std::unique_lock<std::mutex> lock(iterators_mutex_);
    active_iterators_++;
  }

  void DecrementActiveIterators() {
    std::unique_lock<std::mutex> lock(iterators_mutex_);
    active_iterators_--;
    iterators_cv_.notify_all();
  }
  BPlusTree(int _maxCapacity = 4, leveldb::DBImpl* db_impl = nullptr)
      : db_impl_(db_impl) {
    root = new Node(nullptr, true, nullptr, nullptr);
    maxCapacity = _maxCapacity > 2 ? _maxCapacity : 2;
    minCapacity = maxCapacity / 2;
    depth = 0;
    active_iterators_ = 0;
    db_impl_iter=db_impl;
  }

  Node *root;
  int maxCapacity;
  int minCapacity;
  int depth;
  leveldb::DBImpl* db_impl_;
  static leveldb::DBImpl* db_impl_iter;
  struct Internalkeyinfo{
    Slice key;
    std::string user_key;
    uint64_t sequence;
    uint8_t type;
    string value;
  };


  Internalkeyinfo Decodetypeinbptree(const leveldb::Slice& key){
    Internalkeyinfo internalkeyinfo;
    std::string user_key;
    uint64_t sequence;
    uint8_t type;
    const char* p = key.data();
    size_t key_size = key.size();
    size_t user_key_size = key_size - 8;
    size_t sequence_size = 8;
    size_t type_size = 1;
    uint64_t num = DecodeFixed64(p + key_size - 8);
    // 将每个部分的值从字符串中解析出来
    internalkeyinfo.user_key = std::string(p, user_key_size);
    internalkeyinfo.sequence = num >> 8;
    internalkeyinfo.type = static_cast<uint8_t>(p[user_key_size]);
    internalkeyinfo.key=key;
    return(internalkeyinfo);

  }
  Node *findLeaf(string key) {
    Node *node = root;
    while (!node->isLeaf) {
      node = node->getChild(key);
    }
    return node;
  }
  bool get_frombptree(Slice tmpkey,string* getvalue,Status* s) {
    auto start = std::chrono::high_resolution_clock::now();
    Node::NodeInfo* kvpair = (get(tmpkey.ToString(), s));
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    // 输出计时结果
    std::cout << "Bptree search time : " << duration << " nanoseconds" << std::endl;
    if (s->IsNotFound()) {
      *s = Status::NotFound(Slice());
      //cout<<"not find in tree491"<<endl;
      return false;
    } else {
      if (kvpair->internalkeytype == 1 && kvpair->true_value) {
        getvalue->assign(kvpair->value);
        // cout << "line 257 getvalue:" << *getvalue << endl;
        cout<<"存在B+TREE中"<<endl;
        return true;
      } else if(kvpair->internalkeytype == 1 && !kvpair->true_value){
        //指针读取
        start = std::chrono::high_resolution_clock::now();
        *s =  db_impl_->GetValueFromSSTable(kvpair->sst_num, tmpkey,kvpair->offset, getvalue,kvpair->blocksize);
        // 计时结束
         end = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        // 输出计时结果
        std::cout << "Bptree GetValueFromSSTable time : " << duration << " nanoseconds" << std::endl;
        if (s->ok()) {
          cout<<"通过指针找到"<<endl;
          return true;
        } else {
          cout<<"没通过指针找到"<<endl;
          //cout<<"sst_num"<<kvpair->sst_num<<endl;
          //cout<<"offset"<<kvpair->offset<<endl;
          return false;
        }

      }else if (kvpair->internalkeytype == 0) {
       // cout << "key:" << kvpair->key << "是墓碑标记" << endl;
        *s = Status::NotFound(Slice());
        return true;
      }
    }

    return false;
  }
  Node::NodeInfo* get(string key,Status* s) {
    Node::NodeInfo* nodeinfo=findLeaf(key)->get(key,s);
    //cout<<"290 line"<<nodeinfo.key<<endl;
    return nodeinfo ;
  }
  void rangescan(string range_minkey,string range_maxkey) {  findLeaf(range_minkey)->rangescan(range_minkey,range_maxkey); }
  void load_set(string key, Node::NodeInfo* tmpnodeInfo) {
    Node *leaf = findLeaf(key);
    leaf->load_set(key,tmpnodeInfo);
    if (leaf->keys.size() > maxCapacity) {
      insert(leaf->splitLeaf());
    }
  }
  void normal_insertbptree(const Slice &tmpkey, const Slice &tmpvalue){

    Internalkeyinfo internalkeyinfo=Decodetypeinbptree(tmpkey);
    normal_set(internalkeyinfo.user_key,tmpvalue.ToString(),internalkeyinfo.type,internalkeyinfo.sequence);

  }
  void normal_set(string key, string value,uint8_t &internalkeytype,uint64_t &internalkeysequence) {
    cout<<"bptree insertkey: "<<key<<endl;
    //cout<<"bptree insertkey: "<<key<<"value: "<<value.substr(0, 10)<<endl;
    //cout<<"insert key="<<key<<"value="<<value.substr(0, 10)<<"type="<<static_cast<uint32_t>(internalkeytype)<<endl;
    Node *leaf = findLeaf(key);
    leaf->normal_set(key, value, internalkeytype,internalkeysequence);
    if (leaf->keys.size() > maxCapacity) {
      insert(leaf->splitLeaf());
    }
  }
  void hotkey_set(string key, uint64_t sst_num, uint64_t offset,uint8_t internalkeytype,uint64_t internalkeysequence,uint64_t datasize) {
    //cout<<"key"<<key<<"value:"<<value<<endl;
    //cout<<"insert key="<<key<<"value="<<value.substr(0, 10)<<"type="<<static_cast<uint32_t>(internalkeytype)<<endl;
    Node *leaf = findLeaf(key);
    leaf->hotkey_set(key, sst_num, offset,internalkeytype,internalkeysequence,datasize);
    if (leaf->keys.size() > maxCapacity) {
      insert(leaf->splitLeaf());
    }
  }

  void insert(tuple<string, Node *, Node *> result) {
    string key = std::get<0>(result);
    Node *left = std::get<1>(result);
    Node *right = std::get<2>(result);
    Node *parent = right->parent;
    if (parent == nullptr) {
      left->parent = right->parent = root =
          new Node(nullptr, false, nullptr, nullptr);
      depth += 1;
      root->keys = {key};
      root->children = {left, right};
      return;
    }
    parent->setChild(key, {left, right});
    if (parent->keys.size() > maxCapacity) {
      insert(parent->splitInternal());
    }
  }

  void removeFromLeaf(string key, Node *node) {
    int index = node->indexOfKey(key);
    if (index == -1) {
      cout << "Key " << key << " not found in bplustree ! Exiting ..." << endl;
      //未找到键值对的接口
      //exit(0);
      return;
    }else {
      totalMemoryUsage=totalMemoryUsage-node->keys[index].size()-node->nodeInfo[index]->value.size()-6;
      Node::NodeInfo* tmpNodeInfo = node->nodeInfo[index];
      node->keys.erase(node->keys.begin() + index);
      delete tmpNodeInfo;
      node->nodeInfo.erase(node->nodeInfo.begin() + index);
      if (!node->keys.empty() && node->parent) {
        int indexInParent = node->parent->indexOfChild(key);
        if (indexInParent != 0) {
          node->parent->keys[indexInParent - 1] = node->keys.front();
        }
      }
      else if (node->keys.size() < minCapacity && node != root) {
        Node *next = node->next;
        Node *prev = node->prev;
        if (next && next->parent == node->parent &&next->keys.size() > minCapacity) {
          borrowKeyFromRightLeaf(node, next);
        } else if (prev && prev->parent == node->parent &&prev->keys.size() > minCapacity) {
          borrowKeyFromLeftLeaf(node, prev);
        } else if (next && next->parent == node->parent) {
          mergeNodeWithRightLeaf(node, next);
        } else if (prev && prev->parent == node->parent) {
          mergeNodeWithLeftLeaf(node, prev);
        }
      }
      // cout<<"删除key成功："<<key<<endl;

    }

  }

  void removeFromInternal(string key, Node *node) {
    int index = node->indexOfKey(key);
    if (index != -1) {
      Node *leftMostLeaf = node->children[index + 1];
      while (!leftMostLeaf->isLeaf)
        leftMostLeaf = leftMostLeaf->children.front();
      node->keys[index] = leftMostLeaf->keys.front();
    }
  }
  void borrowKeyFromRightLeaf(Node *node, Node *next) {
    node->keys.push_back(next->keys.front());
    next->keys.erase(next->keys.begin());
    node->nodeInfo.push_back(next->nodeInfo.front());
    next->nodeInfo.erase(next->nodeInfo.begin());
    for (int i = 0; i < node->parent->children.size(); i++) {
      if (node->parent->children[i] == next) {
        if (i != 0) {
          node->parent->keys[i - 1] = next->keys.front();
        }                break;
      }}}
  void borrowKeyFromLeftLeaf(Node *node, Node *prev) {
    if (prev->nodeInfo.empty()) {
      node->nodeInfo.clear();
    } else {
      node->nodeInfo.insert(node->nodeInfo.begin(), prev->nodeInfo.back());
      prev->nodeInfo.pop_back();
    }
    node->keys.insert(node->keys.begin(), prev->keys.back());
    prev->keys.pop_back();

    for (int i = 1; i < node->parent->children.size(); i++) {
      if (node->parent->children[i] == node) {
        if (i != 0) {
          node->parent->keys[i - 1] = node->keys.front();
        }
        //cout<<"line 243 break"<<endl;
        break;
      }
    }
  }

  void mergeNodeWithRightLeaf(Node* node, Node* next) {
    node->keys.insert(node->keys.end(), next->keys.begin(), next->keys.end());
    node->nodeInfo.insert(node->nodeInfo.end(), next->nodeInfo.begin(), next->nodeInfo.end());
    node->next = next->next;
    if (node->next) node->next->prev = node;
    for (int i = 0; i < next->children.size(); i++) {
      next->children[i]->parent = node;
      node->children.push_back(next->children[i]);
    }
    next->children.clear();
    for (int i = 0; i < node->parent->children.size(); i++) {
      if (node->parent->children[i] == next) {
        node->parent->keys.erase(node->parent->keys.begin() + i - 1);
        node->parent->children.erase(node->parent->children.begin() + i);
        break;}
    }
    node->nodeInfo.resize(node->keys.size());
  }

  void mergeNodeWithLeftLeaf(Node* node, Node* prev) {
    prev->keys.insert(prev->keys.end(), node->keys.begin(), node->keys.end());
    prev->nodeInfo.insert(prev->nodeInfo.end(), node->nodeInfo.begin(), node->nodeInfo.end());
    prev->next = node->next;
    if (prev->next) prev->next->prev = prev;
    for (int i = 0; i < node->children.size(); i++) {
      node->children[i]->parent = prev;
      prev->children.push_back(node->children[i]);
    }
    prev->children.clear();
    for (int i = 0; i < node->parent->children.size(); i++) {
      if (node->parent->children[i] == node) {
        node->parent->keys.erase(node->parent->keys.begin() + i - 1);
        node->parent->children.erase(node->parent->children.begin() + i);
        break;
      }
    }
    prev->nodeInfo.resize(prev->keys.size());
  }

  void borrowKeyFromRightInternal(int myPositionInParent, Node *node,
                                  Node *next) {
    node->keys.insert(node->keys.end(), node->parent->keys[myPositionInParent]);
    node->parent->keys[myPositionInParent] = next->keys.front();
    next->keys.erase(next->keys.begin());
    node->children.insert(node->children.end(), next->children.front());
    next->children.erase(next->children.begin());
    node->children.back()->parent = node;
  }
  void borrowKeyFromLeftInternal(int myPositionInParent, Node *node, Node *prev) {
    node->keys.insert(node->keys.begin(), node->parent->keys[myPositionInParent - 1]);
    node->parent->keys[myPositionInParent - 1] = prev->keys.back();
    prev->keys.erase(prev->keys.end() - 1);
    node->children.insert(node->children.begin(), prev->children.back());
    prev->children.erase(prev->children.end() - 1);
    node->children.front()->parent = node;
  }

  void mergeNodeWithRightInternal(int myPositionInParent, Node *node, Node *next) {
    node->keys.insert(node->keys.end(), node->parent->keys[myPositionInParent]);
    node->parent->keys.erase(node->parent->keys.begin() + myPositionInParent);
    node->parent->children.erase(node->parent->children.begin() + myPositionInParent + 1);
    node->keys.insert(node->keys.end(), next->keys.begin(), next->keys.end());
    node->children.insert(node->children.end(), next->children.begin(), next->children.end());
    for (Node *child : node->children) {
      child->parent = node;
    }
  }

  void mergeNodeWithLeftInternal(int myPositionInParent, Node *node, Node *prev) {
    prev->keys.insert(prev->keys.end(), node->parent->keys[myPositionInParent - 1]);
    node->parent->keys.erase(node->parent->keys.begin() + myPositionInParent - 1);
    node->parent->children.erase(node->parent->children.begin() + myPositionInParent);
    prev->keys.insert(prev->keys.end(), node->keys.begin(), node->keys.end());
    prev->children.insert(prev->children.end(), node->children.begin(), node->children.end());
    for (Node *child : prev->children) {
      child->parent = prev;
    }
  }

  void remove(string key, Node *node = nullptr) {
    if (node == nullptr) {
      node = findLeaf(key);
    }
    if (node->isLeaf) {
      removeFromLeaf(key, node);
    } else {
      removeFromInternal(key, node);
    }

    if (node->keys.size() < minCapacity) {
      if (node == root) {
        if (root->keys.empty() && !root->children.empty()) {
          root = root->children[0];
          root->parent = nullptr;
          depth -= 1;
        }
        return;
      }else if (node->isLeaf) {
        Node *next = node->next;
        Node *prev = node->prev;

        if (next && next->parent == node->parent &&next->keys.size() > minCapacity) {
          borrowKeyFromRightLeaf(node, next);
        }
        else if (prev && prev->parent == node->parent &&prev->keys.size() > minCapacity) {
          borrowKeyFromLeftLeaf(node, prev);
        }

        if (node->prev != nullptr && node->prev->parent == node->parent &&node->prev->keys.size() > minCapacity) {
          borrowKeyFromRightLeaf(node->prev, node);
        }
        else if (next && next->parent == node->parent &&next->keys.size() <= minCapacity) {
          mergeNodeWithRightLeaf(node, next);
        } else if (prev && prev->parent == node->parent &&prev->keys.size() <= minCapacity) {
          mergeNodeWithLeftLeaf(node, prev);
        }
      } else {
        int myPositionInParent = -1;
        for (int i = 0; i < node->parent->children.size(); i++) {
          if (node->parent->children[i] == node) {
            myPositionInParent = i;
            break;
          }
        }
        Node *next;
        Node *prev;

        if (node->parent->children.size() > myPositionInParent + 1) {
          next = node->parent->children[myPositionInParent + 1];
        } else {
          next = nullptr;
        }

        if (myPositionInParent) {
          prev = node->parent->children[myPositionInParent - 1];
        } else {
          prev = nullptr;
        }

        if (next && next->parent == node->parent &&next->keys.size() > minCapacity) {
          borrowKeyFromRightInternal(myPositionInParent, node, next);
        }

        else if (prev && prev->parent == node->parent &&prev->keys.size() > minCapacity) {
          borrowKeyFromLeftInternal(myPositionInParent,node, prev);
        }

        if (node->prev != nullptr && node->prev->parent == node->parent &&node->prev->keys.size() > minCapacity) {
          mergeNodeWithRightInternal(myPositionInParent,node->prev, node);
        }

        else if (next && next->parent == node->parent &&next->keys.size() <= minCapacity) {
          mergeNodeWithRightInternal(myPositionInParent, node, next);
        }

        else if (prev && prev->parent == node->parent &&prev->keys.size() <= minCapacity) {
          mergeNodeWithLeftInternal(myPositionInParent, node, prev);
        }
      }
    }
    if (node->parent) {
      remove(key, node->parent);
    }
  }


  void print(Node *node = nullptr, string _prefix = "", bool _last = true) {
    if (!node) node = root;
    cout << _prefix << "├ [";
    for (int i = 0; i < node->keys.size(); i++) {
      cout <<node->keys[i];
      if(node->isLeaf) cout<<" true value:"<<node->nodeInfo[i]->true_value;
      if (i != node->keys.size() - 1) {
        cout << ", ";
      }
    }
    cout << "]" << endl;

    _prefix += _last ? "   " : "╎  ";

    if (!node->isLeaf) {
      for (int i = 0; i < node->children.size(); i++) {
        bool _last1 = (i == node->children.size() - 1);
        print(node->children[i], _prefix, _last1);
      }
    }
  }
  void random_access(Node *node) {
    if (node == nullptr ||  !node) {
      return;
    }
    if (node->isLeaf) {
      int size = node->keys.size();
      if (size > 0) {
        int index = rand() % size;
        // cout << node->keys[index] << ": " << node->nodeInfo[index].last_optime << endl;
        average_optime += node->nodeInfo[index]->last_optime;
        random_access_count++;
        // cout<<random_access_count<<endl;
      }
    } else {
      int size = node->children.size();
      if (size > 0) {
        int index = rand() % size;
        random_access(node->children[index]);
      }
    }
  }

  void randomaccess_bptree(int count){
    if(root== nullptr){
      cout<<"根节点为空"<<endl;
    }
    srand(time(NULL));
    for(int i=0;i<count;i++) random_access(root);
  }

  bool get_frombptree_threadsafe(Slice tmpkey, string *getvalue, Status *s) {
    // std::unique_lock<std::mutex> lock(iterators_mutex_);
    // iterators_cv_.wait(lock, [this]() { return active_iterators_ == 0; });
    //std::lock_guard<std::mutex> lock(*mtx);
    return get_frombptree(tmpkey, getvalue, s);
//    lock.unlock();
  }

  void normal_insertbptree_threadsafe(const Slice &tmpkey, const Slice &tmpvalue) {
    std::unique_lock<std::mutex> lock(iterators_mutex_);
    iterators_cv_.wait(lock, [this]() { return active_iterators_ == 0; });
    //std::lock_guard<std::mutex> lock(*mtx);
    normal_insertbptree(tmpkey, tmpvalue);
    lock.unlock();
  }

  void remove_threadsafe(const string &key, Node *node = nullptr) {
    std::unique_lock<std::mutex> lock(iterators_mutex_);
    iterators_cv_.wait(lock, [this]() { return active_iterators_ == 0; });

    remove(key, node);
    lock.unlock();
  }
  void randomaccess_bptree_threadsafe(int count) {
    //std::unique_lock<std::mutex> lock(iterators_mutex_);
    // iterators_cv_.wait(lock, [this]() { return active_iterators_ == 0; });
    //std::lock_guard<std::mutex> lock(*mtx);
    randomaccess_bptree(count);
    //lock.unlock();
  }
  void saveToFile(const std::string &fileName);
  void loadFromFile(const std::string &fileName);
 private:
  std::atomic<int> active_iterators_;
  mutable std::mutex iterators_mutex_;
  std::condition_variable iterators_cv_;


};

class BPlusTreeIterator : public Iterator {
 public:
  explicit BPlusTreeIterator(BPlusTree* tree) : tree_(tree), current_node_(nullptr), current_index_(0) {
    tree_->IncrementActiveIterators();
    SeekToFirst();
  }

  virtual  ~BPlusTreeIterator() {
    tree_->DecrementActiveIterators();
  }
  virtual bool Valid() const {
    //if(current_node_ != nullptr){cout<<"current_node_ != nullptr"<<endl;}else{cout<<"nullptr"<<endl;}
    //if(current_index_ < current_node_->keys.size()){cout<<"current_index_ < current_node_->keys.size()"<<endl;}else{cout<<"index wrong"<<current_index_<<" "<<current_node_->keys.size()<<endl;}
    return current_node_ != nullptr && current_index_ < current_node_->keys.size();
  }

  virtual void SeekToFirst() {
    current_node_ = tree_->findLeaf("");
    current_index_ = 0;
  }
  //跳过热索引
  virtual void SeekToFirst_skiphotindex() {
    current_node_ = tree_->findLeaf("");
    current_index_ = 0;
    if(!current_node_->nodeInfo[current_index_]->true_value) Next();
  }
  virtual void SeekToLast() {
    current_node_ = tree_->findLeaf("");
    while (current_node_->next) {
      current_node_ = current_node_->next;
    }
    current_index_ = current_node_->keys.size() - 1;
  }

  virtual void Seek(const leveldb::Slice& target) {
    //std::unique_lock<std::mutex> lock(*mtx);
    current_node_ = tree_->findLeaf(target.ToString());
    current_index_ = current_node_->indexOfKey(target.ToString());
    if (current_index_ == -1) {
      current_index_ = 0;
    }
    // 如果当前索引对应的 key 小于目标 key，则递增索引直到找到一个大于等于目标 key 的 key
    while (Valid() && current_node_->keys[current_index_] < target.ToString()) {
      current_index_++;
      if (current_index_ == current_node_->keys.size() && current_node_->next && Valid()) {
        current_node_ = current_node_->next;
        current_index_ = 0;
      }
    }
  }
  virtual void Seek_skiphotindex(const leveldb::Slice& target,bool &isresetting_fs_key) {
    //std::unique_lock<std::mutex> lock(*mtx);
    current_node_ = tree_->findLeaf(target.ToString());
    current_index_ = current_node_->indexOfKey(target.ToString());
    if (current_index_ == -1) {
      current_index_ = 0;
    }
    // 如果当前索引对应的 key 小于目标 key，则递增索引直到找到一个大于等于目标 key 的 key
    while (Valid() && current_node_->keys[current_index_] < target.ToString()) {
      current_index_++;
      if (current_index_ == current_node_->keys.size() && current_node_->next) {
        current_node_ = current_node_->next;
        current_index_ = 0;
      }
    }
    if(!Valid()) {
      SeekToFirst_skiphotindex();
      isresetting_fs_key=true;
    }else if(!current_node_->nodeInfo[current_index_]->true_value) Next();
  }
  virtual void Seek_skipTrueKV(const leveldb::Slice& target) {
    //std::unique_lock<std::mutex> lock(*mtx);
    current_node_ = tree_->findLeaf(target.ToString());
    current_index_ = current_node_->indexOfKey(target.ToString());
    if (current_index_ == -1) {
      current_index_ = 0;
    }
    // 如果当前索引对应的 key 小于目标 key，则递增索引直到找到一个大于等于目标 key 的 key
    while (Valid() && current_node_->keys[current_index_] < target.ToString()) {
      current_index_++;
      if (current_index_ == current_node_->keys.size() && current_node_->next) {
        current_node_ = current_node_->next;
        current_index_ = 0;
      }
    }
    if(current_node_->nodeInfo[current_index_]->true_value) H_Next();
  }
  virtual void Next() {
    if (Valid()) {
      current_index_++;
      if (current_index_ == current_node_->keys.size() && current_node_->next) {
        current_node_ = current_node_->next;
        current_index_ = 0;
      }
    }
  }
  virtual void FlushNext() {
    if (Valid()) {
      current_index_++;
      if (current_index_ == current_node_->keys.size() && current_node_->next) {
        current_node_ = current_node_->next;
        current_index_ = 0;
      }
      //热树索引
      if(Valid() && !current_node_->nodeInfo[current_index_]->true_value ){
        FlushNext();
      }
    }
  }
  //遍历热树索引
  virtual void H_Next()  {
    if (Valid()) {
      current_index_++;
      if (current_index_ == current_node_->keys.size() && current_node_->next) {
        current_node_ = current_node_->next;
        current_index_ = 0;
      }
      if( Valid() && current_node_->nodeInfo[current_index_]->true_value  ){
        H_Next();
      }
    }
  }
  //序列化使用
  virtual void Save_Next() {
    if (Valid()) {
      current_index_++;
      if (current_index_ == current_node_->keys.size() && current_node_->next) {
        current_node_ = current_node_->next;
        current_index_ = 0;
      }

    }
  }
  virtual void Prev() {
    //std::unique_lock<std::mutex> lock(*mtx);
    if (Valid()) {
      if (current_index_ == 0) {
        if (current_node_->prev) {
          current_node_ = current_node_->prev;
          current_index_ = current_node_->keys.size() - 1;
        }
      } else {
        current_index_--;
      }
      if(!current_node_->nodeInfo[current_index_]->true_value  && Valid()){
        Prev();
      }
    }
  }
  mutable std::string internal_key_;
  virtual leveldb::Slice key() const {
    //std::unique_lock<std::mutex> lock(*mtx);
    if (Valid()) {
      internal_key_ = Encodeinternalkeyinbptree(current_node_->keys[current_index_], current_node_->nodeInfo[current_index_]);
      return leveldb::Slice(internal_key_);

    }
    return leveldb::Slice();
  }
  virtual string keystring() const {
    //std::unique_lock<std::mutex> lock(*mtx);
    if (Valid()) {

      return current_node_->keys[current_index_];

    }
    return "";
  }

  virtual leveldb::Slice value() const {
    //std::unique_lock<std::mutex> lock(*mtx);
    if (Valid()) {
      if (current_node_->nodeInfo[current_index_]->true_value) {
        return leveldb::Slice(current_node_->nodeInfo[current_index_]->value);
      }else{
        string getvalue;
       Status s =  BPlusTree::db_impl_iter->GetValueFromSSTable(current_node_->nodeInfo[current_index_]->sst_num, current_node_->keys[current_index_],current_node_->nodeInfo[current_index_]->offset, &getvalue,current_node_->nodeInfo[current_index_]->blocksize);
        return leveldb::Slice(getvalue);
      }
    }
    return leveldb::Slice();
  }
  uint64_t  last_optime() const {
    //std::unique_lock<std::mutex> lock(*mtx);
    if (Valid()) {
      return current_node_->nodeInfo[current_index_]->last_optime;
    }
    return 0;
  }
  virtual leveldb::Status status() const {
    if (Valid()) {
      return leveldb::Status::OK();
    }
    return leveldb::Status::NotFound(leveldb::Slice());
  }
  string Encodeinternalkeyinbptree (string key,Node::NodeInfo* tmpinfo) const{
    string res;
    ParsedInternalKey bpinternalkey;
    bpinternalkey=ParsedInternalKey(Slice(key),tmpinfo->internalkeysequence,static_cast<ValueType>(tmpinfo->internalkeytype));
    AppendInternalKey(&res,bpinternalkey);
    return res;
  }
  Node* current_node_;
  int current_index_;
 private:
  BPlusTree* tree_;
};



}
#endif  // LEVELDB_BPTREE_H

