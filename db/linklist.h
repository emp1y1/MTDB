  #ifndef LEVELDB_LINKLIST_H
  #define LEVELDB_LINKLIST_H
  #include <string>
  #include <fstream>
  #include <iostream>
  using namespace std;
  namespace  leveldb {
  class LinkedList {
   public:

    std::string max_data;
    std::string min_data;
    size_t size_;
    struct ListNode {
      std::string data;
      int sst_num=0;
      ListNode* prev;
      ListNode* next;
      int l0_kv_num=0;
      int op_num=0;
      ListNode(const std::string& key) : data(key), prev(nullptr), next(nullptr) {}
    };

    LinkedList() : head_(nullptr), size_(0) {}

    ~LinkedList() {
      ListNode* current = head_;
      while (current != nullptr) {
        ListNode* next_node = current->next;
        delete current;
        current = next_node;
      }
    }

    bool IsEmpty() const {
      return head_ == nullptr;
    }
    void Insert(const std::string& key) {
      ListNode* new_node = new ListNode(key);
      ListNode* current = head_;
      ListNode* previous = nullptr;

      while (current != nullptr && current->data < key) {
        previous = current;
        current = current->next;
      }

      if (previous == nullptr) {
        new_node->next = head_;
        if (head_ != nullptr) {
          head_->prev = new_node;
        }
        head_ = new_node;
      } else {
        new_node->prev = previous;
        new_node->next = current;
        previous->next = new_node;
        if (current != nullptr) {
          current->prev = new_node;
        }
      }
      size_++;
      // 更新 max_data_
      if (current == nullptr) {
        max_data = key;
      }

      // 更新 min_data_
      if (head_ == new_node) {
        min_data = key;
      }
    }
    uint64_t FindMaxSstNum() {
      if (IsEmpty()) {
        return 0;
      }

      uint64_t max_sst_num = head_->sst_num;
      ListNode* current = head_->next;

      while (current != nullptr) {
        if (current->sst_num > max_sst_num) {
          max_sst_num = current->sst_num;
        }
        current = current->next;
      }

      return max_sst_num;
    }
    pair<string,string> ResetNodesInRange(const std::string& rangeStart, const std::string& rangeEnd) {
      ListNode* current = FindFirstLargerOrEqualKey(rangeStart);
      pair<string,string> range;
      range.first = current->data;
      while (current != nullptr && current->data <= rangeEnd) {
        current->op_num = 0;
        current->l0_kv_num = 0;
        current = current->next;
        range.second = current->data;
      }
      return  range;
    }
    void Remove(const std::string& key) {
      ListNode* current = head_;
      ListNode* previous = nullptr;

      while (current != nullptr && current->data != key) {
        previous = current;
        current = current->next;
      }

      if (current == nullptr) {
        return;
      }

      if (previous == nullptr) {
        head_ = current->next;
        if (head_ != nullptr) {
          head_->prev = nullptr;
        }
      } else {
        previous->next = current->next;
        if (current->next != nullptr) {
          current->next->prev = previous;
        }
      }
      delete current;
      // 更新 size_
      size_--;
    }
    void Print() const {
      ListNode* current = head_;
      while (current != nullptr) {
        //cout << current->data << " -> ";
        current = current->next;
      }
      cout << "nullptr" << std::endl;
    }
      void Print_num() const {
      ListNode* current = head_;
      while (current != nullptr) {
        cout << current->data<<" : "<<current->sst_num << " -> ";
        current = current->next;
      }
      cout << "nullptr" << std::endl;
    }
    int Print_toalnum() const {
      ListNode* current = head_;
      int totalnum=0;
      while (current != nullptr) {
        totalnum+=current->sst_num;
        current = current->next;
      }
      return totalnum;
    }
    int SumGreaterEqualSstNum(int totalscore) {
      int sum = 0;
      ListNode* current = head_;

      while (current != nullptr) {
        if (current->sst_num >= totalscore) {
          sum += current->sst_num;
        }
        current = current->next;
      }

      return sum;
    }
    ListNode*  GreaterEqualSstNum(int score) {
      int sum = 0;
      ListNode* current = head_;

      while (current != nullptr) {
        if (current->sst_num > score) {
          return current;
        }
        current = current->next;
      }

      return nullptr;
    }
    //找到第一个大于的key
    LinkedList::ListNode* FindNode(const std::string& key) {
      if (key.empty()) {
       // std::cerr << "错误：传递给 FindNode 的 key 为空！" << std::endl;
      } else {
        //std::cout << "传递给 FindNode 的 key: " << key << std::endl;
      }
      if (head_ == nullptr) {
        return nullptr;
      }

      ListNode* current = head_;
      ListNode* previous = nullptr;

      while (current != nullptr && current->data <= key) {
        current = current->next;
      }

      // 如果 current 为 nullptr，说明没有找到大于等于 key 的节点，返回链表的最后一个节点（小于给定 key 的最大节点）
      if (current == nullptr) {
        return head_;
      }


      // 返回找到的节点（小于等于给定 key 的最大链表 key 的位置）
      return current;
    }

    LinkedList::ListNode* Find_SSTnum(const std::string& key) {
      ListNode* node = FindNode(key);

      if (node != nullptr) {
        node->sst_num++;
      }

      return node;
    }

    ListNode* FindFirstLargerOrEqualKey(const std::string key) const {

      ListNode* current = head_;
      // 如果链表为空，直接返回 nullptr
      if (current == nullptr) {
        return nullptr;
      }

      // 遍历链表，找到第一个大于等于 key 的节点
      while (current != nullptr && current->data < key) {
        current = current->next;
      }

      // 如果 current 为 nullptr，说明没有找到大于等于 key 的节点，返回链表的第一个节点
      if (current == nullptr) {
        return nullptr;
      }

      // 返回找到的节点
      return current;
    }

    ListNode* FindFirstLargerKey(const std::string& key) const {
      ListNode* current = head_;
      // 如果链表为空，直接返回 nullptr
      if (current == nullptr) {
        return nullptr;
      }

      // 遍历链表，找到第一个大于 key 的节点
      while (current != nullptr && current->data <= key) {
        current = current->next;
      }

      // 如果 current 为 nullptr，说明没有找到大于 key 的节点，返回 nullptr
      if (current == nullptr) {
        return head_;
      }

      // 返回找到的节点
      return current;
    }

    bool Serialize(const std::string& filename) {
      std::ofstream outfile(filename, std::ios::binary | std::ios::trunc);
      if (!outfile) {
        return false;
      }

      // Save LinkedList member variables
      outfile << max_data << std::endl;
      outfile << min_data << std::endl;
      outfile.write(reinterpret_cast<const char*>(&size_), sizeof(size_));

      ListNode* current = head_;
      while (current) {
        size_t len = current->data.size();
        outfile.write(reinterpret_cast<const char*>(&len), sizeof(len));
        outfile.write(current->data.data(), len);

        // Save all other members of ListNode
        outfile.write(reinterpret_cast<const char*>(&current->sst_num), sizeof(current->sst_num));
        outfile.write(reinterpret_cast<const char*>(&current->l0_kv_num), sizeof(current->l0_kv_num));
        outfile.write(reinterpret_cast<const char*>(&current->op_num), sizeof(current->op_num));

        current = current->next;
      }

      outfile.close();
      return true;
    }

    bool Deserialize(const std::string& filename) {
      std::ifstream infile(filename, std::ios::binary);
      if (!infile) {
        return false;
      }

      // Load LinkedList member variables
      std::getline(infile, max_data);
      std::getline(infile, min_data);
      infile.read(reinterpret_cast<char*>(&size_), sizeof(size_));

      while (!infile.eof()) {
        size_t len;
        infile.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (infile) {
          std::string value(len, '\0');
          infile.read(&value[0], len);

          // Create new node and load all its members
          ListNode* new_node = new ListNode(value);
          infile.read(reinterpret_cast<char*>(&new_node->sst_num), sizeof(new_node->sst_num));
          infile.read(reinterpret_cast<char*>(&new_node->l0_kv_num), sizeof(new_node->l0_kv_num));
          infile.read(reinterpret_cast<char*>(&new_node->op_num), sizeof(new_node->op_num));

          // Insert the new node
          // Note: this assumes the nodes in the file are sorted in the same order as in the list
          ListNode* current = head_;
          ListNode* previous = nullptr;

          while (current != nullptr && current->data < new_node->data) {
            previous = current;
            current = current->next;
          }

          if (previous == nullptr) {
            new_node->next = head_;
            if (head_ != nullptr) {
              head_->prev = new_node;
            }
            head_ = new_node;
          } else {
            new_node->prev = previous;
            new_node->next = current;
            previous->next = new_node;
            if (current != nullptr) {
              current->prev = new_node;
            }
          }
        }
      }

      infile.close();
      return true;
    }



   public:
    ListNode* head_;
  };
  }


  #endif