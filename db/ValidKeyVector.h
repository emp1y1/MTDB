//
// Created by 林鑫伟 on 2023/4/10.
//

#ifndef LEVELDB_VALIDKEYVECTOR_H
#define LEVELDB_VALIDKEYVECTOR_H
namespace leveldb {
extern vector<pair<std::string, std::string>> hotrange;
struct ValidKeyStruct {
  uint32_t total_num;
  uint32_t valid_num;
  std::string mixkey;
  std::string maxkey;
};
class ValidKeyVector {
 public:
  void AddStruct(const ValidKeyStruct& ms) { my_structs_.push_back(ms); }
  std::pair<std::string, std::string> FindColdinHotTree(const ValidKeyVector* valid_key_vector) {
    uint32_t min_ratio = UINT32_MAX;
    std::string min_mixkey;
    std::string min_maxkey;

    for (const auto& valid_key_struct : valid_key_vector->my_structs_) {
      uint32_t ratio = valid_key_struct.valid_num * 100 / valid_key_struct.total_num;
      if (ratio < min_ratio) {
        min_ratio = ratio;
        min_mixkey = valid_key_struct.mixkey;
        min_maxkey = valid_key_struct.maxkey;
      }
    }

    return std::make_pair(min_mixkey, min_maxkey);
  }
  bool Serialize(const std::string& filename) {
    std::ofstream ofs(filename, std::ios::binary);
    if (!ofs.is_open()) {
      return false;
    }
    for (const auto& ms : my_structs_) {
      ofs.write(reinterpret_cast<const char*>(&ms), sizeof(ValidKeyStruct));
    }
    return true;
  }

  bool Deserialize(const std::string& filename) {
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs.is_open()) {
      return false;
    }
    ValidKeyStruct ms;
    while (ifs.read(reinterpret_cast<char*>(&ms), sizeof(ValidKeyStruct))) {
      my_structs_.push_back(ms);
    }
    return true;
  }

  const std::vector<ValidKeyStruct>& GetStructs() const { return my_structs_; }


  std::vector<ValidKeyStruct> my_structs_;
};
class RequestTracker {
 public:
  RequestTracker(size_t buffer_size, const std::vector<std::pair<std::string, std::string>>& key_ranges)
      : buffer_size_(buffer_size), read_count_(0), write_count_(0), buffer_pos_(0), request_count_(0), key_ranges_(key_ranges) {
    buffer_.resize(buffer_size);
    for (const auto& range : key_ranges) {
      range_counter_[range] = 0;
    }
  }

  void RecordRead(const std::string& key) {

    UpdateBuffer(true, key);
    read_count_++;
    IncrementRangeCounter(key);
    ProcessRequest();
  }

  void RecordWrite(const std::string& key) {

    UpdateBuffer(false, key);
    write_count_++;
    IncrementRangeCounter(key);
    ProcessRequest();
  }

  double GetReadWriteRatio() {

    if (write_count_ == 0) {
      return read_count_;
    }
    return static_cast<double>(read_count_) / write_count_;
  }

  std::map<std::pair<std::string, std::string>, int> GetRangeRequestCounts() {

    return range_counter_;
  }


  void UpdateBuffer(bool is_read, const std::string& key) {
    auto old_entry = buffer_[buffer_pos_];
    if (old_entry.first) {
      read_count_--;
    } else {
      write_count_--;
    }
    buffer_[buffer_pos_] = std::make_pair(is_read, key);
    buffer_pos_ = (buffer_pos_ + 1) % buffer_size_;
  }

  void IncrementRangeCounter(const std::string& key) {
    for (const auto& range : key_ranges_) {
      if (key >= range.first && key < range.second) {
        range_counter_[range]++;
        break;
      }
    }
  }

  pair<string,string> ProcessRequest() {
    request_count_++;
    //if (request_count_ >= buffer_size_) {
      //std::cout << "Read/Write Ratio: " << GetReadWriteRatio() << std::endl;
     //判断
      auto range_counts = GetRangeRequestCounts();
      auto max_range = std::max_element(range_counts.begin(), range_counts.end(),
                                        [](const std::pair<const std::pair<string, string>, int>& a,
                                           const std::pair<const std::pair<string, string>, int>& b) {
                                          return a.second < b.second;
                                        });
      return max_range->first;

      //request_count_ = 0;
    //}
  }

  size_t buffer_size_;
  int read_count_;
  int write_count_;
  size_t buffer_pos_;
  size_t request_count_;
  std::vector<std::pair<bool, std::string>> buffer_;
  std::vector<std::pair<std::string, std::string>> key_ranges_;
  std::map<std::pair<std::string, std::string>, int> range_counter_;
  std::mutex mutex_;
};


}
#endif  // LEVELDB_VALIDKEYVECTOR_H
