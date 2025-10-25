// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"
#include <iostream>
using namespace std;
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "db/bptree.h"
#include <sys/resource.h>
#include "linklist.h"
#include "leveldb/iterator.h"
#include "util/arena.h"
#include "leveldb/cache.h"
#include "table/block_builder.h"
#include "leveldb/filter_policy.h"
#include "table/format.h"
#include "bptree.h"
#include "ValidKeyVector.h"
#include <sstream>
#include <iomanip>
#include <chrono>

namespace leveldb {
//BPlusTree* tree;
LinkedList* partition;
vector<pair<std::string, std::string>> hotrange;
DBImpl::SSTableInfoManager* HotTree_manager;
//ValidKeyVector* ValidKey;
RequestTracker* request_tracker_;
leveldb::DBImpl* leveldb::BPlusTree::db_impl_iter = nullptr;
void BPlusTree::saveToFile(const std::string &fileName) {
  //std::ofstream outFile(fileName, std::ios::binary);
  std::ofstream outFile(fileName, std::ios::binary | std::ios::trunc);
  if (!outFile) {
    std::cerr << "Error opening file for writing: " << fileName << std::endl;
    return;
  }

  BPlusTreeIterator iterator(this);
  //iterator.SeekToFirst();
  while (iterator.Valid()) {
    auto key = iterator.keystring();

    Node::NodeInfo *nodeInfo = iterator.current_node_->nodeInfo[iterator.current_index_];

    uint32_t keySize = key.size();
    outFile.write(reinterpret_cast<const char *>(&keySize), sizeof(keySize));
    outFile.write(key.c_str(), keySize);

    uint32_t valueSize = nodeInfo->value.size();
    outFile.write(reinterpret_cast<const char *>(&valueSize), sizeof(valueSize));
    outFile.write(nodeInfo->value.c_str(), valueSize);

    // Save NodeInfo data
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->last_optime), sizeof(nodeInfo->last_optime));
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->range_num_Node), sizeof(nodeInfo->range_num_Node));
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->true_value), sizeof(nodeInfo->true_value));
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->internalkeysequence), sizeof(nodeInfo->internalkeysequence));
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->internalkeytype), sizeof(nodeInfo->internalkeytype));
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->sst_num), sizeof(nodeInfo->sst_num));
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->offset), sizeof(nodeInfo->offset));
    outFile.write(reinterpret_cast<const char *>(&nodeInfo->blocksize), sizeof(nodeInfo->blocksize));

    iterator.Save_Next();
  }

  outFile.close();
}

void BPlusTree::loadFromFile(const std::string &fileName) {
  std::ifstream inFile(fileName, std::ios::binary);
  if (!inFile) {
    std::cerr << "Error opening file for reading: " << fileName << std::endl;
    return;
  }

  while (!inFile.eof()) {



    uint32_t keySize;
    inFile.read(reinterpret_cast<char *>(&keySize), sizeof(keySize));
    if (inFile.eof()) break;
    std::string key(keySize, '\0');
    inFile.read(&key[0], keySize);


    // Load NodeInfo data
    Node::NodeInfo nodeInfo("");
    uint32_t valueSize;
    inFile.read(reinterpret_cast<char *>(&valueSize), sizeof(valueSize));
    nodeInfo.value.resize(valueSize);
    inFile.read(&nodeInfo.value[0], valueSize);
    inFile.read(reinterpret_cast<char *>(&nodeInfo.last_optime), sizeof(nodeInfo.last_optime));
    inFile.read(reinterpret_cast<char *>(&nodeInfo.range_num_Node), sizeof(nodeInfo.range_num_Node));
    inFile.read(reinterpret_cast<char *>(&nodeInfo.true_value), sizeof(nodeInfo.true_value));
    inFile.read(reinterpret_cast<char *>(&nodeInfo.internalkeysequence), sizeof(nodeInfo.internalkeysequence));
    inFile.read(reinterpret_cast<char *>(&nodeInfo.internalkeytype), sizeof(nodeInfo.internalkeytype));
    inFile.read(reinterpret_cast<char *>(&nodeInfo.sst_num), sizeof(nodeInfo.sst_num));
    inFile.read(reinterpret_cast<char *>(&nodeInfo.offset), sizeof(nodeInfo.offset));
    inFile.read(reinterpret_cast<char *>(&nodeInfo.blocksize), sizeof(nodeInfo.blocksize));

    load_set(key, &nodeInfo);
  }

  inFile.close();
}

void get_memory_usage(size_t *vm_size, size_t *rss) {
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  *vm_size = usage.ru_maxrss * 1024;
  *rss = usage.ru_idrss * 1024;
}

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(64 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      tree(new BPlusTree(3, this)),
     // partition(new LinkedList()),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_, this)) {
  partition = new LinkedList();
  HotTree_manager = new DBImpl::SSTableInfoManager() ;
  //ValidKey=new ValidKeyVector();
  std::vector<std::pair<std::string, std::string>> key_ranges;
  for (int i = 0; i < 99; ++i) {
    std::stringstream ss1;
    ss1 << std::setw(2) << std::setfill('0') << i;
    std::string key1 = "user" + ss1.str();
    while (key1.length() < 24) {
      key1 += "0";
    }
    std::stringstream ss2;
    ss2 << std::setw(2) << std::setfill('0') << i+1;
    std::string key2 = "user" + ss2.str();
    while (key2.length() < 24) {
      key2 += "0";
    }
    key_ranges.push_back(std::make_pair(key1, key2));
  }
  key_ranges.push_back(std::make_pair("user9900000000000000000000","user99999999999999999999" ));
  key_ranges.push_back(std::make_pair("user99999999999999999999", "v"));
  request_tracker_=new RequestTracker(1000,key_ranges);
  partition_maxkey="";
  fs_key="";
  //fs_key=""; //last flush stop key;

}


DBImpl::~DBImpl() {
  cout << "~DBImpl()" << endl;
  cout << "version l0 tnumber" << versions_->NumLevelFiles(0) << endl;
  cout << "linklist number" << partition->Print_toalnum() << endl;
  cout<<"totalMemoryUsage:"<<totalMemoryUsage<<endl;
  cout<<"hotkeyindexMemoryUsage:"<<hotkeyindexMemoryUsage<<endl;
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  while(totalMemoryUsage>50000000) {
    CompactBplusTreeWhenClose();
  }
  //leveldb::Version* current_version = versions_->current();
  int count = 0;
  cout << "begin" << endl;
  while (LinkedList::ListNode* partition_node=partition->GreaterEqualSstNum(config::kL0_CompactionTrigger) ){
    //if(partition_node->data == "user99999999999999999999" || versions_->current()->compaction_level_ != 0) break;
    if(partition_node->data == "user99999999999999999999" || versions_->current()->compaction_level_ != 0) break;
    cout<<"~dbimpl找到的分区maxkey ："<<partition_node->data<<" num : "<<partition_node->sst_num;
      InternalKey smallest;
      InternalKey largest;
      std::vector<FileMetaData*> compact_files;

      // 查找要压缩的分区
      bool found_compaction = versions_->FindCompactionPartitionbykey(partition_node->data,&compact_files,smallest,largest);
      if(found_compaction) {
      partition_node->sst_num = 0;
      bool insert=false;
      if(partition->size_ < 500){
      std::string max_key = partition_node->data;
      std::string min_key = partition_node->prev->data;
      while (max_key.length() < 24) {
        max_key += "0";
      }
      while (min_key.length() < 24) {
        min_key += "0";
      }

      min_key = min_key.substr(4);  // strip "user" prefix
      max_key = max_key.substr(4);  // strip "user" prefix

      std::string res = "";
      int i = min_key.size() - 1, j = max_key.size() - 1, carry = 0;
      while (i >= 0 || j >= 0 || carry) {
        long sum = 0;
        if (i >= 0) {
          sum += (min_key[i] - '0');
          i--;
        }
        if (j >= 0) {
          sum += (max_key[j] - '0');
          j--;
        }
        sum += carry;
        carry = sum / 10;
        sum = sum % 10;
        res = res + char(sum + '0');
      }
      std::reverse(res.begin(), res.end());

      std::string ans;
      int idx = 0;
      int temp = res[idx] - '0';
      while (temp < 2) temp = temp * 10 + (res[++idx] - '0');
      while (res.size() > idx) {
        ans += (temp / 2) + '0';
        temp = (temp % 2) * 10 + res[++idx] - '0';
      }

      if (ans.length() == 0) std::string mid_key = "user0";

      std::string mid_key = "user" + ans;
      partition->Insert(mid_key);
      cout << "maxkey" << max_key;
      cout << "minkey" << min_key;
      cout << "新节点" << mid_key << endl;
      insert=true;
       }
      partition->Print_num();
      int this_op_num = partition_node->op_num + partition_node->l0_kv_num;
      // 更新前执行热度判断策略
      cout << partition_node->data << endl;
      cout << partition_node->prev->data << endl;
      string key1 = partition_node->data.substr(4, 8);  // strip "user" prefix
      string key2 =
          partition_node->prev->data.substr(4, 8);  // strip "user" prefix
      cout << "key1 : " << key1 << endl;
      cout << "key2 : " << key2 << endl;
      cout << "key1- key2 : " << std::stoi(key1) - std::stoi(key2) << endl;
      cout << "this_op_num" << this_op_num << endl;
      float par_hotness =
          this_op_num / (static_cast<float>(std::stoi(key1)) - std::stoi(key2));
      cout << "par_hotness= " << par_hotness << endl;
      Slice smallestkey = smallest.user_key();
      Slice largestkey = largest.user_key();
      string smallestkeystring = smallest.user_key().ToString();
      string largestkeystring = largest.user_key().ToString();
      cout << "smallestkeystring: " << smallestkeystring << endl;
      cout << "largestkeystring: " << largestkeystring << endl;
      // 热度符合条件
      if (par_hotness > 0) {
        // 查找层级添加文件
        versions_->GetAlllOverlapping(&smallest, &largest, &compact_files);
        // 执行热度转移
        cout << "执行热度转移" << endl;
        {
          mutex_.Unlock();
          ProcessSSTablesForHotKeys(&compact_files, smallestkeystring,
                                    largestkeystring);
          mutex_.Lock();
        }
        partition_node->op_num = 0;
        partition_node->l0_kv_num = 0;
/*        if (compact_files.empty()) exit(0);
                for (auto it = compact_files.rbegin(); it !=
           compact_files.rend(); ++it) { FileMetaData* file = *it; std::cout <<
           "~DBIMPL Number: " << file->number << ", MinKey: " <<
           file->smallest.DebugString()
                            << ", MaxKey: " << file->largest.DebugString() <<
           std::endl;
                }*/
        // hotrange.push_back(std::make_pair(smallest.user_key().ToString(),largest.user_key().ToString()));
      } else {
        {
          mutex_.Unlock();

          DBImpl::TEST_CompactRange(0, &smallestkey, &largestkey);
          Log(options_.info_log, "调用了TEST_CompactRange，smallestkey：%s，largestkey：%s",
              smallestkey.ToString().c_str(), largestkey.ToString().c_str());
          mutex_.Lock();
        }
        while (background_compaction_scheduled_) {
          background_work_finished_signal_.Wait();
        }
        if(insert) {
          partition_node->op_num = this_op_num / 2;
          partition_node->prev->op_num = this_op_num / 2;
          partition_node->l0_kv_num = 0;
        }else{partition_node->l0_kv_num = 0;}
      }
      }

}
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }
  cout<<"level 0 sstable : "<<versions_->NumLevelFiles(0)<<endl;
  cout<<"level 1 sstable : "<<versions_->NumLevelFiles(1)<<endl;
  cout<<"level 2 sstable : "<<versions_->NumLevelFiles(2)<<endl;
  //cout<<"level 3 sstable : "<<versions_->NumLevelFiles(3)<<endl;
  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  //free tree;
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;
  totalMemoryUsage=0;
  HotTree_manager->TraverseSSTables([](uint64_t sst_number, const DBImpl::SSTableInfo& sst_info) {
    std::cout << "SSTable Number: " << sst_number
              << ", Size: " << sst_info.size
              << ", Min Key: " << sst_info.min_key
              << ", Max Key: " << sst_info.max_key
              << std::endl;});
  tree->saveToFile(dbname_ + "/" + "BPTree.txt");
  //tree->print();
  delete tree;
  tree = nullptr;
  partition->Serialize(dbname_ + "/" + "partition_list.txt");
  partition->Print_num();
  delete partition;
  partition= nullptr;

  HotTree_manager->Serialize(dbname_ + "/" + "HotTree_list.txt");
  //ValidKey->Serialize(dbname_ + "/" + "ValidKey.txt");
  delete HotTree_manager;
  //delete ValidKey;
  HotTree_manager = nullptr;
  //ValidKey = nullptr;
  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}
struct KeyBlockAddress {
  std::string key;
  uint64_t block_address;
  size_t offset;
};

void DBImpl::ReadTable(FileMetaData* file, Table** table_ptr) {
  // Read the table from the file
  Options options;
  Status s;
  RandomAccessFile* file_ptr;
  uint64_t file_size = file->file_size;
  s = env_->NewRandomAccessFile(TableFileName(dbname_, file->number), &file_ptr);
  if (!s.ok()) {
    // Log error and return
    cout<<"find失败"<<file->number<<endl;
    return;
  }
  //cout<<"find成功"<<endl;
  s = Table::Open(options, file_ptr, file_size, table_ptr);
  if (!s.ok()) {
    cout<<"open失败"<<endl;
    // Log error and return
    return;
  }
}

void DBImpl::ProcessSSTablesForHotKeys(std::vector<FileMetaData*>* inputs,string &minkey,string &maxkey) {
  MutexLock lock(&mutex_);
  VersionEdit edit;
  cout<<"line 274"<<endl;
  leveldb::Version* current_version = versions_->current();
  //current_version->Ref();
  struct Valueinfo{
    uint64_t sequence;
    uint8_t type;
    uint64_t number;
    uint64_t offset;
    uint64_t blocksize;
  };
  // 1. Read and merge SSTables
  std::map<std::string, Valueinfo> key_to_file_offset;
  ReadOptions read_options;
  Slice key;
  ParsedInternalKey resultkey;
  uint32_t num=0;
  for (auto it = inputs->rbegin(); it != inputs->rend(); ++it) {
    FileMetaData* file = *it;
    Table* table_ptr;
    std::cout << "ProcessSSTablesForHotKeys Number: " << file->number << ", MinKey: " << file->smallest.DebugString()
              << ", MaxKey: " << file->largest.DebugString() <<", size: " << file->file_size<< std::endl;
    ReadTable(file, &table_ptr);
    Iterator* table_iter = table_ptr->NewIterator(read_options);

    // 2. Add SSTable info to SSTableInfoManager,保存热树sstable信息

     SSTableInfo sst_info;
     sst_info.size = file->file_size;
     sst_info.min_key = file->smallest.Encode().ToString();
     sst_info.max_key = file->largest.Encode().ToString();
     HotTree_manager->AddSSTable(file->number, sst_info);



    //3.output <key, block address> pairs
    for (table_iter->SeekToFirst(); table_iter->Valid(); table_iter->Next()) {
      Valueinfo valueinfo;
      key = table_iter->key();
      bool parse_ok = ParseInternalKey(key, &resultkey);
      if (parse_ok) {
        // 解析成功，可以使用 parsed_key 对象了
        valueinfo.sequence = resultkey.sequence;
        valueinfo.type = resultkey.type;
        // ...
      //cout<<"line 286key: "<<key.ToString()<<endl;
        BlockHandle handle=table_ptr->ApproximateOffsetAndSizeOf(key);
        valueinfo.offset = handle.offset();
        valueinfo.number = file->number ;
        valueinfo.blocksize=handle.size();
        key_to_file_offset[resultkey.user_key.ToString()] = valueinfo; // 将新的对象存储到映射中
        num++;//计算键值对个数
      }
    }

    delete table_iter;
    delete table_ptr;
  }
  //ValidKeyStruct new_struct = {num, 0, minkey, maxkey};
  //ValidKey->AddStruct(new_struct);
  cout<<"num: "<<num<<endl;
  // 4. Remove SSTables from the version
  for (int level = 0; level < GetNumLevels(current_version); level++) {
    std::vector<uint64_t> to_remove;
    for (const auto& input : *inputs) {
      const std::vector<FileMetaData*>& files = GetFileList(current_version, level);
      auto it = std::find_if(files.begin(), files.end(), [&](const FileMetaData* f) { return f->number == input->number; });
      if (it != files.end()) {
        to_remove.push_back(input->number);
      }
    }
   // current->Ref();
    for (auto sst_number : to_remove) {
      // 使用新的操作类型
      //edit.RemoveFileWithoutDeleting(level, sst_number);
      edit.RemoveFile(level, sst_number);
      versions_->current()->RemoveSSTableFromVersion(level, sst_number);
    }

   // current->Unref();

  // Make sure the remaining files in the Version are still sorted by key range.
    //versions_->current()->SortFilesBySmallestKey(level, internal_comparator_);
  }

  cout<<"line 323"<<endl;



  //background_compaction_scheduled_ = false;
  // 插入B+tree,key_to_file_offset
  for (const auto& p : key_to_file_offset) {
   // std::cout << "Key: " << p.first << ", Value: " << p.second.number<< std::endl;
    tree->hotkey_set(p.first,p.second.number,p.second.offset,p.second.type,p.second.sequence,p.second.blocksize);
    //string value;
    //Status s=GetValueFromSSTable(p.second.number,p.first,p.second.offset,&value,p.second.blocksize);
    //cout<<"test offsetfind key: "<<p.first<<" value:"<<value<<endl;
    //cout << " status:" << s.ToString() << endl;
  }

  //将修改应用到manifest
  //current_version->Unref();
  Status s;
  {
    s = versions_->LogAndApply(&edit, &mutex_);
    if (!s.ok()) {
      // handle error
    }
  }

  range_num++;
}

Status DBImpl::GetValueFromSSTable(uint64_t file_number, const Slice& key, uint64_t offset, std::string* value,uint64_t blocksize) {
  auto start = std::chrono::high_resolution_clock::now();
  Status s;
  FileMetaData file_meta;
  file_meta.number = file_number;
  env_->GetFileSize(TableFileName(dbname_, file_meta.number), &file_meta.file_size);
  Table* table = nullptr;
  DBImpl::ReadTable(&file_meta, &table);
  if (table == nullptr) {
    return Status::NotFound("Table not found for file_number.");
  }
 // cout<<"key："<<key.ToString()<<endl;
  //cout<<"file_number:"<<file_number<<endl;
  //cout<<"file size:"<<file_size<<endl;
  //cout<<"找到table"<<endl;
  // Get a block iterator for the specific offset.
  ReadOptions read_options;
  Iterator* block_iter = table->NewIteratorFromOffset(read_options, offset,blocksize);
  block_iter->Seek(key);
  //cout<<"iter key："<<block_iter->key().ToString()<<endl;
   //cout<<"value"<<block_iter->value().ToString()<<endl;
  if (block_iter->Valid() && ExtractUserKey(block_iter->key()) == key) {
    *value = block_iter->value().ToString();
  } else {
    s = Status::NotFound("Key: "+block_iter->key().ToString()+" not found in the table.");
    exit(0);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  // 输出计时结果
  std::cout << "GetValueFromSSTable internal time: " << duration << " nanoseconds" << std::endl;
  delete block_iter;
  delete table;
  return s;

}
/*
Status DBImpl::GetValueFromSSTable(uint64_t file_number, const Slice& key, uint64_t offset, std::string* value,uint64_t file_size) {
  FileMetaData file_meta;
  file_meta.number = file_number;
  file_meta.file_size=file_size;

  Table* table = nullptr;
  ReadTable(&file_meta, &table);
  if (table == nullptr) {
    return Status::NotFound("Table not found for file_number.");
  }

  // Get a block iterator for the specific offset.
  ReadOptions read_options;
  Iterator* block_iter = table->NewIteratorFromApproximateOffset(read_options, offset);
  block_iter->Seek(key);

  Status s;
  if (block_iter->Valid() && block_iter->key() == key) {
    *value = block_iter->value().ToString();
  } else {
    s = Status::NotFound("Key not found in the table.");
  }

  delete block_iter;
  delete table;
  return s;
}
*/

int DBImpl::GetNumLevels(leveldb::Version* version) {
  return config::kNumLevels;
}

const std::vector<FileMetaData*>& DBImpl::GetFileList(leveldb::Version* version, int level) {
  return version->GetFileList(level);
}


void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}


void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
/*          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()) || (number >= versions_->FlushBeginLogNumber()));*/
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          // Check if the file is in the live set or in the HotTree_manager
          keep = (live.find(number) != live.end()) || (HotTree_manager->sst_info_map_.find(number) != HotTree_manager->sst_info_map_.end())
                 || (filename.substr(filename.size() - 4) == ".txt");
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        Log(options_.info_log, "Delete type=%d #%lld filename=%s\n", static_cast<int>(type),
            static_cast<unsigned long long>(number), filename.c_str());
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        /*Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));*/
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}


Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str());
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFiletoTree(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

/*    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);*/
    status = WriteBatchInternal::InsertInto(&batch, tree);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;

    }
  }



  return status;
}
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      cout<<"460line"<<endl;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      cout<<"500line"<<endl;
    }
    mem->Unref();
  }

  return status;
}
//lxw
struct Internalkeyinfo{
  Slice key;
  std::string user_key;
  uint64_t sequence;
  uint8_t type;
  string value;
};
Internalkeyinfo DecodeInternalkey(const leveldb::Slice key){
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
  //std::cout << "user_key: " << user_key << std::endl;
  // std::cout << "sequence: " << sequence << std::endl;
  //std::cout << "type: " << static_cast<uint32_t>(type) << std::endl;

}
Status DBImpl::WriteBptree(MemTable* mem) {
  Iterator* iter = mem->NewIterator();
  iter->SeekToFirst();
  while (iter->Valid()) {
    Slice internalkey = iter->key();
    Slice value = iter->value();
    string key = DecodeInternalkey(internalkey).user_key;
    uint64_t sequence = DecodeInternalkey(internalkey).sequence;
    uint8_t type = DecodeInternalkey(internalkey).type;

    uint64_t max_sequence = sequence;
    Slice max_sequence_value = value;
    Slice max_sequence_internalkey = internalkey;

    while (true) {
      iter->Next();

      // break the loop if iterator is not valid, meaning end of entries
      if (!iter->Valid()) {
        break;
      }

      string next_key = DecodeInternalkey(iter->key()).user_key;
      uint64_t next_sequence = DecodeInternalkey(iter->key()).sequence;

      // break the loop if the next key is not equal to the current key
      if (key.compare(next_key) != 0) {
        break;
      }

      if (next_sequence >= max_sequence) {
        max_sequence = next_sequence;
        max_sequence_value = iter->value();
        max_sequence_internalkey = iter->key();
      }
    }

    // insert the max_sequence entry into the tree
    tree->normal_insertbptree_threadsafe(max_sequence_internalkey, max_sequence_value);
  }

  Status s = leveldb::Status::OK();
  return s;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  cout<<"WriteLevel0Table is Called"<<endl;
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    //lxw:table_cache在缓存刚刚落盘的L0文件，应该要把此功能给删除
    // s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    //lxw:测试执行PickLevelForMemTableOutput前后的性能差异，再做判断
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}
Status DBImpl::WriteLevel0TablefromHotTree(VersionEdit* edit,Version* base) {
  mutex_.AssertHeld();
  vector<string> deletekey;
//遍历Vector，或者范围扫描vecto中的范围，找到minkey和maxkey
  BPlusTreeIterator* bp_iter = new BPlusTreeIterator(tree);
  int maxtime=0;
  pair<string,string> coldestrange;
  for (auto it = hotrange.begin(); it != hotrange.end(); ++it) {
    std::string min = it->first;
    std::string max = it->second;
    int opt=0;
    int tempavetime=0;
    for(bp_iter->Seek_skipTrueKV(min);bp_iter->keystring()<=max;bp_iter->H_Next()){
      tempavetime += bp_iter->last_optime();
      opt++;
    }
    tempavetime= tempavetime / opt;
    if(tempavetime > maxtime) {
      coldestrange.first=min;
      coldestrange.second=max;
    }
  }

  //当更新密集或者写密集时
  //判断有效值个数
  //std::pair<std::string, std::string> ColdRange=ValidKey->FindColdinHotTree(ValidKey);
  //读密集判断时间戳，未实现！an'z
//在manager中删除这部分的文件
  string minkey=coldestrange.first;
  string maxkey=coldestrange.second;
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Status s;
  BPlusTreeIterator* bpt_iter = new BPlusTreeIterator(tree);
  //leveldb::Iterator* iter = bpt_iter;
 // iter->Seek(Slice(minkey));
  bp_iter->Seek_skipTrueKV(minkey);
  while( maxkey>bpt_iter->keystring()) {
    Log(options_.info_log, "Level-0 table from HotTree #%llu: started",
        (unsigned long long)meta.number);

    {
      mutex_.Unlock();
      // lxw:table_cache在缓存刚刚落盘的L0文件，应该要把此功能给删除
      s = BuildTablefromHotTree(dbname_, env_, options_, table_cache_, bp_iter,
                                &meta, &deletekey, minkey, maxkey,partition);
      // cout << " status:" << s.ToString() << endl;
      mutex_.Lock();
    }

    Log(options_.info_log, "L0 file #%llu: size %llu, min_key %s, max_key %s",
        static_cast<unsigned long long>(meta.number),
        static_cast<unsigned long long>(meta.file_size),
        meta.smallest.user_key().ToString().substr(0, 10).c_str(),
        meta.largest.user_key().ToString().substr(0, 10).c_str());

    // delete iter;
    pending_outputs_.erase(meta.number);


    int level = 0;
    if (s.ok() && meta.file_size > 0) {
      const Slice min_user_key = meta.smallest.user_key();
      const Slice max_user_key = meta.largest.user_key();
      // lxw:测试执行PickLevelForMemTableOutput前后的性能差异，再做判断
      if (base != nullptr) {
        level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
      }
      // cout << "sstable写入的level" << level << "min: "<<min_user_key.ToString()<<"max: "<<max_user_key.ToString()<<endl;
      LinkedList::ListNode* tmpnode =
          partition->FindFirstLargerOrEqualKey(max_user_key.ToString());
      if (level == 0) {
        cout << "File " << meta.number << "  生成SST size: " << meta.file_size
             << "  min： " << meta.smallest.user_key().ToString()
             << "  max :" << meta.largest.user_key().ToString() << endl;
        cout << "将"
             << partition->FindFirstLargerOrEqualKey(max_user_key.ToString())
                    ->data
             << " 的num从"
             << partition->FindFirstLargerOrEqualKey(max_user_key.ToString())
                    ->sst_num;
        tmpnode->sst_num++;
        tmpnode->l0_kv_num += deletekey.size();
        // tmpnode->op_num+=deletekey.size();
        cout << " 修改为： "
             << partition->FindFirstLargerOrEqualKey(max_user_key.ToString())
                    ->sst_num
             << endl;
        if (partition->FindFirstLargerOrEqualKey(max_user_key.ToString())
                ->data < max_user_key.ToString()) {
          cout << "partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->data < max_user_key.ToString()"
               << endl;
          exit(0);
        }
        // partition->Print_num();
      } else {
        tmpnode->op_num += deletekey.size();
      }
              cout<<"lin1199";
      /*
            cout<<"meta.number"<<meta.number<<endl;
            cout<<"meta.largest"<<meta.largest.user_key().ToString()<<endl;
          cout<<"meta.smallest"<<meta.smallest.user_key().ToString()<<endl;
          cout<<"meta.number"<<meta.number<<endl;
          if(!edit) cout<<"edit no exit"<<endl;*/
      edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                    meta.largest);
    }

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros;
    stats.bytes_written = meta.file_size;
    stats_[level].Add(stats);
    // cout<<stats.micros<<endl;
    // cout<<stats.bytes_written<<endl;
    // cout << "  status:" << s.ToString() << endl;
    /*  size_t vector_size = deletekey.size();
      for (size_t i = vector_size; i > 0; --i) {
        // 从 B+树中删除键
        tree->remove_threadsafe(deletekey[i - 1]);
      }*/
  }
  for (const std::string& key : deletekey) {
    // 从 B+树中删除键
    tree->remove_threadsafe(key);
  }
  HotTree_manager->RemoveSSTablesInRange(minkey, maxkey);
  delete bpt_iter;
  //iter = nullptr;
  return s;
}
Status DBImpl::WriteLevel0Tablefrombptree(VersionEdit* edit,Version* base,LinkedList::ListNode** tmpnode) {
  if(tree->root == nullptr) versions_->ExistFileNumber();
  cout<<"WriteLevel0Tablefrombptree is Called"<<endl;
  mutex_.AssertHeld();
  vector<string> deletekey;
  cout<<"totalMemoryUsage:"<<totalMemoryUsage<<endl;
  //cout<<tree->random_access_count<<endl;
  random_access_count = 0;
  tree->randomaccess_bptree_threadsafe(15);
  string tmpfs_key=fs_key;
  //cout<<"668 : "<<average_optime<<endl;
  // cout<<"668 : "<<random_access_count<<endl;
  if(random_access_count == 0)
  {
    cout<<"random accessbptree is fall."<<random_access_count<<endl;
    //cout<totalMemoryUsage<<endl;
    //return Status::Corruption("random accessbptree is fall.");
    average_optime=9999999999999999;
    //tree->print();
  }else{
    average_optime=average_optime/random_access_count;
   // average_optime=9999999999999999;
  }
//cout<<average_optime<<endl;
 // string tmppartition_maxkey;
  cout<<"average_optime :"<<average_optime<<endl;
  //average_optime=999999999999999;

  //补充获取partition_maxkey，即此次flush的最大键，最小键（起始位置）为fs_key记录；
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  //cout<<"line 655"<<endl;
  //tree->rangescan("0","z");
  //tree->bpflush(partition_maxkey);
  //cout<<"line 657"<<endl;
  BPlusTreeIterator* bpt_iter = new BPlusTreeIterator(tree);
  leveldb::Iterator* iter = bpt_iter;
  bool isresetting_fs_key=false;

/*   for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
     iter->key();
     //std::cout << "Key: " << iter->key().ToString() << ", Value: " << iter->value().ToString() << std::endl;
      std::cout << "Key: " << iter->key().ToString().substr(0, 10) <<  std::endl;
   }*/

  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    //lxw:table_cache在缓存刚刚落盘的L0文件，应该要把此功能给删除
    s = BuildTablefrombptree(dbname_, env_, options_, table_cache_, iter, &meta,average_optime,&deletekey,partition,&partition_maxkey,&fs_key,isresetting_fs_key);
    //cout << " status:" << s.ToString() << endl;
    mutex_.Lock();
  }
  //cout<<"bulidTABLE后的partition_maxkey:"<<partition_maxkey<<endl;
  //partition_maxkey =tmppartition_maxkey;
  //cout<<partition_maxkey<<endl;
/*    Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
        (unsigned long long)meta.number, (unsigned long long)meta.file_size,
        s.ToString().c_str());*/
  Log(options_.info_log, "L0 file #%llu: size %llu, min_key %s, max_key %s",
      static_cast<unsigned long long>(meta.number),
      static_cast<unsigned long long>(meta.file_size),
      meta.smallest.user_key().ToString().substr(0,10).c_str(),
      meta.largest.user_key().ToString().substr(0,10).c_str());
  if(tmpfs_key == fs_key) fs_key=meta.largest.user_key().ToString();
  if(isresetting_fs_key) versions_->ExistFileNumber();
  cout<<"更新后的fs_key: "<<fs_key<<endl;
  // if (!deletekey.empty()) {
    //std::cout << "deletekey First element: " << deletekey.front().substr(0,10) << std::endl;std::cout << "deletekey Last element: " << deletekey.back().substr(0,10)  << std::endl;
 // }
  //if(!iter->Valid()){
    //cout<<"迭代器失效，重置fs_key和partition_maxkey"<<endl;
    //fs_key="";
    //partition_maxkey = partition->min_data;
  //}
  //cout<<"fs_key: "<<fs_key.substr(0,10)<<endl;
  delete bpt_iter;
  iter=nullptr;
 // delete iter;
  pending_outputs_.erase(meta.number);
  for (const std::string& key : deletekey) {
    // 从 B+树中删除键
    tree->remove_threadsafe(key);
  }
  //cout<<partition->FindFirstLargerOrEqualKey((deletekey)[deletekey.size() - 1])->data<<endl;

  //cout<<iter->key().ToString()<<endl;
  //cout<<std::string(iter->key().data(), iter->key().size()-8)<<endl;

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    // lxw:测试执行PickLevelForMemTableOutput前后的性能差异，再做判断
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    //cout << "sstable写入的level" << level << "min: "<<min_user_key.ToString()<<"max: "<<max_user_key.ToString()<<endl;
    *tmpnode =partition->FindFirstLargerOrEqualKey(max_user_key.ToString());
    if (level == 0) {
      cout<<"File "<<meta.number<<"  生成SST size: "<<meta.file_size<<"  min： "<<meta.smallest.user_key().ToString()<<"  max :"<<meta.largest.user_key().ToString()<<endl;
      cout<<"将"<<partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->data<<" 的num从"<<partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->sst_num;
      (*tmpnode)->sst_num++;
      //if(tmpnode->sst_num > 4) BackgroundCall();
      (*tmpnode)->l0_kv_num += deletekey.size();
      //tmpnode->op_num+=deletekey.size();
      cout<<" 修改为： "<<partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->sst_num<<endl;
/*      if(partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->data < max_user_key.ToString()){
        cout<<"partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->data < max_user_key.ToString()"<<endl;
        exit(0);

      }*/
      //partition->Print_num();
    }else{
      cout<<"File "<<meta.number<<"  生成SST size: "<<meta.file_size<<"  min： "<<meta.smallest.user_key().ToString()<<"  max :"<<meta.largest.user_key().ToString()<<endl;
      (*tmpnode)->op_num+=deletekey.size();
    }

    /*
          cout<<"meta.number"<<meta.number<<endl;
          cout<<"meta.largest"<<meta.largest.user_key().ToString()<<endl;
        cout<<"meta.smallest"<<meta.smallest.user_key().ToString()<<endl;
        cout<<"meta.number"<<meta.number<<endl;
        if(!edit) cout<<"edit no exit"<<endl;*/
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);

  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  //cout<<stats.micros<<endl;
  //cout<<stats.bytes_written<<endl;
  //cout << "  status:" << s.ToString() << endl;
/*  size_t vector_size = deletekey.size();
  for (size_t i = vector_size; i > 0; --i) {
    // 从 B+树中删除键
    tree->remove_threadsafe(deletekey[i - 1]);
  }*/

  return s;
}
void DBImpl::CompactBplusTree() {
  mutex_.AssertHeld();
  //assert(totalMemoryUsage >200);
  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  LinkedList::ListNode* tmpnode= nullptr;
  base->Ref();
  Status s = WriteLevel0Tablefrombptree(&edit, base, &tmpnode);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during bptree compaction");
  }
  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }
/*  if(tmpnode != nullptr ) {
    cout << "tmpnode!= nullptr " << endl;
    cout <<"tmpnode->sst_num: "<< tmpnode->sst_num << endl;
    if (tmpnode->sst_num > 6) {
      int count = 0;
      cout << "pick begin" << endl;

      cout << "~write找到的分区maxkey ：" << tmpnode->data
           << " num : " << tmpnode->sst_num;
      InternalKey smallest;
      InternalKey largest;
      std::vector<FileMetaData*> compact_files;
      // 查找要压缩的分区
      bool found_compaction = versions_->FindCompactionPartitionbyListNode(
          tmpnode, &compact_files, smallest, largest);
      if (found_compaction) {
        std::string max_key = tmpnode->data;
        std::string min_key = tmpnode->prev->data;
        tmpnode->sst_num = 0;
        min_key = min_key.substr(4);  // strip "user" prefix
        max_key = max_key.substr(4);  // strip "user" prefix

        std::string res = "";
        int i = min_key.size() - 1, j = max_key.size() - 1, carry = 0;
        while (i >= 0 || j >= 0 || carry) {
          long sum = 0;
          if (i >= 0) {
            sum += (min_key[i] - '0');
            i--;
          }
          if (j >= 0) {
            sum += (max_key[j] - '0');
            j--;
          }
          sum += carry;
          carry = sum / 10;
          sum = sum % 10;
          res = res + char(sum + '0');
        }
        std::reverse(res.begin(), res.end());

        std::string ans;
        int idx = 0;
        int temp = res[idx] - '0';
        while (temp < 2) temp = temp * 10 + (res[++idx] - '0');
        while (res.size() > idx) {
          ans += (temp / 2) + '0';
          temp = (temp % 2) * 10 + res[++idx] - '0';
        }

        if (ans.length() == 0) std::string mid_key = "user0";

        std::string mid_key = "user" + ans;
        partition->Insert(mid_key);
        cout << "maxkey" << max_key;
        cout << "minkey" << min_key;
        cout << "新节点" << mid_key << endl;
        partition->Print_num();
        int this_op_num = tmpnode->op_num + tmpnode->l0_kv_num;
        // 更新前执行热度判断策略
        cout << tmpnode->data << endl;
        cout << tmpnode->prev->data << endl;
        string key1 = tmpnode->data.substr(4, 6);        // strip "user" prefix
        string key2 = tmpnode->prev->data.substr(4, 6);  // strip "user" prefix
        cout << "key1 : " << key1 << endl;
        cout << "key2 : " << key2 << endl;
        cout << "key1- key2 : " << std::stoi(key1) - std::stoi(key2) << endl;
        cout << "this_op_num" << this_op_num << endl;
        float par_hotness = this_op_num / (static_cast<float>(std::stoi(key1)) -
                                           std::stoi(key2));
        cout << "par_hotness= " << par_hotness << endl;
        Slice smallestkey = smallest.user_key();
        Slice largestkey = largest.user_key();
        string smallestkeystring = smallest.user_key().ToString();
        string largestkeystring = largest.user_key().ToString();
        cout << "smallestkeystring: " << smallestkeystring << endl;
        cout << "largestkeystring: " << largestkeystring << endl;
        // 热度符合条件
        if (par_hotness > 1) {
          // 查找层级添加文件
          versions_->GetAlllOverlapping(&smallest, &largest, &compact_files);
          // 执行热度转移
          cout << "执行热度转移" << endl;
          {
            mutex_.Unlock();
          //  ProcessSSTablesForHotKeys(&compact_files, smallestkeystring,
               //                       largestkeystring);
            cout << "执行完毕ProcessSSTablesForHotKeysBybase" << endl;
            mutex_.Lock();
          }
          tmpnode->op_num = 0;
          tmpnode->l0_kv_num = 0;
          *//*        if (compact_files.empty()) exit(0);
                          for (auto it = compact_files.rbegin(); it !=
                     compact_files.rend(); ++it) { FileMetaData* file = *it;
             std::cout <<
                     "~DBIMPL Number: " << file->number << ", MinKey: " <<
                     file->smallest.DebugString()
                                      << ", MaxKey: " <<
             file->largest.DebugString() << std::endl;
                          }*//*
          // hotrange.push_back(std::make_pair(smallest.user_key().ToString(),largest.user_key().ToString()));

        } else {
          {
            mutex_.Unlock();
            DBImpl::TEST_CompactRange(0, &smallestkey, &largestkey);
            mutex_.Lock();
          }
          tmpnode->op_num = this_op_num / 2;
          tmpnode->prev->op_num = this_op_num / 2;
          tmpnode->l0_kv_num = 0;
        }
      }
    }
  }*/
  if (s.ok()) {
    // Commit to the new state
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }

}
Status DBImpl::WriteLevel0TablefrombptreeWhenClose(VersionEdit* edit,Version* base,LinkedList::ListNode** tmpnode) {
  if(tree->root == nullptr) versions_->ExistFileNumber();
  cout<<"WriteLevel0TablefrombptreeWhenClose is Called"<<endl;
  mutex_.AssertHeld();
  vector<string> deletekey;
  cout<<"totalMemoryUsage:"<<totalMemoryUsage<<endl;
  //cout<<tree->random_access_count<<endl;
  string tmpfs_key=fs_key;
  //cout<<"668 : "<<average_optime<<endl;
  // cout<<"668 : "<<random_access_count<<endl;

    average_optime=9999999999999999;


  //average_optime=999999999999999;

  //补充获取partition_maxkey，即此次flush的最大键，最小键（起始位置）为fs_key记录；
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  //cout<<"line 655"<<endl;
  //tree->rangescan("0","z");
  //tree->bpflush(partition_maxkey);
  //cout<<"line 657"<<endl;
  BPlusTreeIterator* bpt_iter = new BPlusTreeIterator(tree);
  leveldb::Iterator* iter = bpt_iter;
  bool isresetting_fs_key=false;

  /*   for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
       iter->key();
       //std::cout << "Key: " << iter->key().ToString() << ", Value: " << iter->value().ToString() << std::endl;
        std::cout << "Key: " << iter->key().ToString().substr(0, 10) <<  std::endl;
     }*/

  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    //lxw:table_cache在缓存刚刚落盘的L0文件，应该要把此功能给删除
    s = BuildTablefrombptree(dbname_, env_, options_, table_cache_, iter, &meta,average_optime,&deletekey,partition,&partition_maxkey,&fs_key,isresetting_fs_key);
    //cout << " status:" << s.ToString() << endl;
    mutex_.Lock();
  }
  //cout<<"bulidTABLE后的partition_maxkey:"<<partition_maxkey<<endl;
  //partition_maxkey =tmppartition_maxkey;
  //cout<<partition_maxkey<<endl;
  /*    Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
          (unsigned long long)meta.number, (unsigned long long)meta.file_size,
          s.ToString().c_str());*/
  Log(options_.info_log, "L0 file #%llu: size %llu, min_key %s, max_key %s",
      static_cast<unsigned long long>(meta.number),
      static_cast<unsigned long long>(meta.file_size),
      meta.smallest.user_key().ToString().substr(0,10).c_str(),
      meta.largest.user_key().ToString().substr(0,10).c_str());
  if(tmpfs_key == fs_key) fs_key=meta.largest.user_key().ToString();
  if(isresetting_fs_key) versions_->ExistFileNumber();
  cout<<"更新后的fs_key: "<<fs_key<<endl;
  // if (!deletekey.empty()) {
  //std::cout << "deletekey First element: " << deletekey.front().substr(0,10) << std::endl;std::cout << "deletekey Last element: " << deletekey.back().substr(0,10)  << std::endl;
  // }
  //if(!iter->Valid()){
  //cout<<"迭代器失效，重置fs_key和partition_maxkey"<<endl;
  //fs_key="";
  //partition_maxkey = partition->min_data;
  //}
  //cout<<"fs_key: "<<fs_key.substr(0,10)<<endl;
  delete bpt_iter;
  iter=nullptr;
  // delete iter;
  pending_outputs_.erase(meta.number);
  for (const std::string& key : deletekey) {
    // 从 B+树中删除键
    tree->remove_threadsafe(key);
  }
  //cout<<partition->FindFirstLargerOrEqualKey((deletekey)[deletekey.size() - 1])->data<<endl;

  //cout<<iter->key().ToString()<<endl;
  //cout<<std::string(iter->key().data(), iter->key().size()-8)<<endl;

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    // lxw:测试执行PickLevelForMemTableOutput前后的性能差异，再做判断
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    //cout << "sstable写入的level" << level << "min: "<<min_user_key.ToString()<<"max: "<<max_user_key.ToString()<<endl;
    *tmpnode =partition->FindFirstLargerOrEqualKey(max_user_key.ToString());
    if (level == 0) {
      cout<<"File "<<meta.number<<"  生成SST size: "<<meta.file_size<<"  min： "<<meta.smallest.user_key().ToString()<<"  max :"<<meta.largest.user_key().ToString()<<endl;
      cout<<"将"<<partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->data<<" 的num从"<<partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->sst_num;
      (*tmpnode)->sst_num++;
      //if(tmpnode->sst_num > 4) BackgroundCall();
      (*tmpnode)->l0_kv_num += deletekey.size();
      //tmpnode->op_num+=deletekey.size();
      cout<<" 修改为： "<<partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->sst_num<<endl;
      /*      if(partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->data < max_user_key.ToString()){
              cout<<"partition->FindFirstLargerOrEqualKey(max_user_key.ToString())->data < max_user_key.ToString()"<<endl;
              exit(0);

            }*/
      //partition->Print_num();
    }else{
      cout<<"File "<<meta.number<<"  生成SST size: "<<meta.file_size<<"  min： "<<meta.smallest.user_key().ToString()<<"  max :"<<meta.largest.user_key().ToString()<<endl;
      (*tmpnode)->op_num+=deletekey.size();
    }

    /*
          cout<<"meta.number"<<meta.number<<endl;
          cout<<"meta.largest"<<meta.largest.user_key().ToString()<<endl;
        cout<<"meta.smallest"<<meta.smallest.user_key().ToString()<<endl;
        cout<<"meta.number"<<meta.number<<endl;
        if(!edit) cout<<"edit no exit"<<endl;*/
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,
                  meta.largest);

  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  //cout<<stats.micros<<endl;
  //cout<<stats.bytes_written<<endl;
  //cout << "  status:" << s.ToString() << endl;
  /*  size_t vector_size = deletekey.size();
    for (size_t i = vector_size; i > 0; --i) {
      // 从 B+树中删除键
      tree->remove_threadsafe(deletekey[i - 1]);
    }*/

  return s;
}
void DBImpl::CompactBplusTreeWhenClose() {
  mutex_.AssertHeld();
  //assert(totalMemoryUsage >200);
  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  LinkedList::ListNode* tmpnode= nullptr;
  base->Ref();
  Status s = WriteLevel0TablefrombptreeWhenClose(&edit, base, &tmpnode);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during bptree compaction");
  }
  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }
  if (s.ok()) {
    // Commit to the new state
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }

}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteBptree(imm_);
  //Status s = WriteLevel0Table(imm_, &edit, base); lxw
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0); //表示没有正在执行compaction的日志文件
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()&&totalMemoryUsage < 50000000) {
    // No work to be done
  }
  else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

  void DBImpl::BackgroundCall() {
    MutexLock l(&mutex_);
    assert(background_compaction_scheduled_);
    if (shutting_down_.load(std::memory_order_acquire)) {
      // No more background work when shutting down.
    } else if (!bg_error_.ok()) {
      // No more background work after a background error.
    } else {
      BackgroundCompaction();
    }

    background_compaction_scheduled_ = false;

    // Previous compaction may have produced too many files in a level,
    // so reschedule another compaction if needed.
    MaybeScheduleCompaction();
    background_work_finished_signal_.SignalAll();
  }
  void DBImpl::DoPickCompaction() {
    mutex_.AssertHeld();
    std::vector<FileMetaData*> compact_files;
    string minkey;
    string maxkey;

    mutex_.AssertHeld();
    Compaction* c;
    cout << "调用了pickcompaction" << endl;
    c = versions_->PickCompaction(compact_files,minkey,maxkey);

    Status status;
    if (c == nullptr) {
      if (!compact_files.empty()) {
      {
        //ProcessSSTablesForHotKeys(&compact_files,minkey,maxkey);
      }
      cout << "lin1140" << endl;
      }
      cout << "c == nullptr" << endl;
      // Nothing to do
    } else if (c->IsTrivialMove()) {
      // Move file to next level
      assert(c->num_input_files(0) == 1);
      FileMetaData* f = c->input(0, 0);
      c->edit()->RemoveFile(c->level(), f->number);
      c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                         f->largest);
      status = versions_->LogAndApply(c->edit(), &mutex_);
      if (!status.ok()) {
      RecordBackgroundError(status);
      }
      VersionSet::LevelSummaryStorage tmp;
      Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
          static_cast<unsigned long long>(f->number), c->level() + 1,
          static_cast<unsigned long long>(f->file_size),
          status.ToString().c_str(), versions_->LevelSummary(&tmp));
    } else {
      CompactionState* compact = new CompactionState(c);
      status = DoCompactionWork(compact);
      if (!status.ok()) {
      RecordBackgroundError(status);
      }

      CleanupCompaction(compact);
      c->ReleaseInputs();
      RemoveObsoleteFiles();
    }
    delete c;

    if (status.ok()) {
      // Done
    } else if (shutting_down_.load(std::memory_order_acquire)) {
      // Ignore compaction errors found during shutting down
    } else {
      Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
    }
  }

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();
  string minkey;
  string maxkey;
  VersionEdit edit;
  if (imm_ != nullptr) {
    CompactMemTable();
    //cout<<"line 854CompactMemTable"<<endl;
    //return;
  }
  if (totalMemoryUsage > 50000000) {
    //int before=totalMemoryUsage;
    //cout<<"before totalMemoryUsage "<<totalMemoryUsage<<endl;
    CompactBplusTree();
    //WriteLevel0Tablefrombptree();
    //cout<<"after totalMemoryUsage"<<before-totalMemoryUsage<<endl;
    return;
  }
  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    cout<<"manual compaction"<<endl;
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else while(partition->GreaterEqualSstNum(config::kL0_CompactionTrigger) != nullptr){

      std::vector<FileMetaData*> compact_files;//find失败
     cout<<"调用了pickcompaction"<<endl;
    c = versions_->PickCompaction(compact_files,minkey,maxkey);



  Status status;
  if (c == nullptr) {
    if(!compact_files.empty()) {

      {
        mutex_.Unlock();
        ProcessSSTablesForHotKeys(&compact_files,minkey,maxkey);
        mutex_.Lock();
      }
      cout<<"lin1140"<<endl;
    }
 cout<<"c == nullptr"<<endl;
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest);
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }

    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  //edit.SetPrevLogNumber(0);
 // edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
  //status = versions_->LogAndApply(&edit, &mutex_);
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
    }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);
  if (compact->compaction->level() == 0) {
    std::cout
        << "L0-L1 compaction: num_files "
        << compact->compaction->num_input_files(0) << ", min_key "
        << compact->compaction->input(0, 0)->smallest.user_key().ToString()
        << ", max_key "
        << compact->compaction
               ->input(0, compact->compaction->num_input_files(0) - 1)
               ->largest.user_key()
               .ToString()
        << std::endl;
    for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
      const leveldb::FileMetaData* f = compact->compaction->input(0, i);
      std::cout << "  " << "File " << f->number
                << " [ " << f->smallest.user_key().ToString()
                << ", " << f->largest.user_key().ToString()
                << " ]" << std::endl;
    }
  }
  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }
    if (totalMemoryUsage > 50000000) {
      mutex_.Lock();
      //int before=totalMemoryUsage;
      //cout<<"before totalMemoryUsage "<<totalMemoryUsage<<endl;
      CompactBplusTree();
      //WriteLevel0Tablefrombptree();
      //cout<<"after totalMemoryUsage"<<before-totalMemoryUsage<<endl;
      background_work_finished_signal_.SignalAll();
      mutex_.Unlock();
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
          0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  if (compact->compaction->level() == 0) {
    Log(options_.info_log,
        "L0-L1 compaction: num_files %d, min_key %s, max_key %s",
        compact->compaction->num_input_files(0),
        compact->compaction->input(0, 0)->smallest.user_key().ToString().c_str(),
        compact->compaction->input(0, compact->compaction->num_input_files(0) - 1)
            ->largest.user_key()
            .ToString()
            .c_str());
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  list.push_back(new BPlusTreeIterator(tree));
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}







Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;

  cout<<"get key: "<< key.ToString()<<endl;

  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Initialize timers
  auto start = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> mem_duration(0), imm_duration(0), tree_duration(0), sst_duration(0);

  {
    mutex_.Unlock();
    LookupKey lkey(key, snapshot);

    // Time mem->Get
    auto mem_start = std::chrono::high_resolution_clock::now();
    if (mem->Get(lkey, value, &s)) {
      cout<<"find in mem"<<endl;
      mem_duration = std::chrono::high_resolution_clock::now() - mem_start;
    } else {
      mem_duration = std::chrono::high_resolution_clock::now() - mem_start;

      // Time imm->Get
      auto imm_start = std::chrono::high_resolution_clock::now();
      if (imm != nullptr && imm->Get(lkey, value, &s)) {
        cout<<"find in imm"<<endl;
        imm_duration = std::chrono::high_resolution_clock::now() - imm_start;
      } else {
        imm_duration = std::chrono::high_resolution_clock::now() - imm_start;

        // Time tree->get_frombptree_threadsafe
      auto tree_start = std::chrono::high_resolution_clock::now();
        if(tree->get_frombptree_threadsafe(key,value,&s)){
          cout<<"find in tree"<<endl;
          tree_duration = std::chrono::high_resolution_clock::now() - tree_start;
        } else {
          tree_duration = std::chrono::high_resolution_clock::now() - tree_start;

          // Time current->Get
          auto sst_start = std::chrono::high_resolution_clock::now();
          s = current->Get(options, lkey, value, &stats);
          have_stat_update = true;
          sst_duration = std::chrono::high_resolution_clock::now() - sst_start;
        }
      }
    }

    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();

  // Calculate total time
  auto total_duration = std::chrono::high_resolution_clock::now() - start;
  auto total_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(total_duration).count();
  auto mem_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(mem_duration).count();
  auto imm_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(imm_duration).count();
  auto tree_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(tree_duration).count();
  auto sst_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(sst_duration).count();

  // Print results
  cout << "Total time: " << total_nano << " nanoseconds" << endl;
  cout << "Mem time: " << mem_nano << " nanoseconds (" << (100.0 * mem_nano / total_nano) << "%)" << endl;
  cout << "Imm time: " << imm_nano << " nanoseconds (" << (100.0 * imm_nano / total_nano) << "%)" << endl;
  cout << "Tree time: " << tree_nano << " nanoseconds (" << (100.0 * tree_nano / total_nano) << "%)" << endl;
  cout << "SST time: " << sst_nano << " nanoseconds (" << (100.0 * sst_nano / total_nano) << "%)" << endl;

  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                        ? static_cast<const SnapshotImpl*>(options.snapshot)
                            ->sequence_number()
                        : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && partition->GreaterEqualSstNum(config::kL0_SlowdownWritesTrigger)!= nullptr && versions_->current()->compaction_level_ == 0) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
/*     cout<<"version l0 tnumber"<<versions_->NumLevelFiles(0) <<endl;
      //cout<<"linklist number"<<partition->Print_toalnum()<<endl;
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
       // Do not delay a single write more than once
      mutex_.Lock();*/
      allow_delay = false;
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      cout<<"imm_ != nullptr"<<endl;
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    }
    else if (partition->GreaterEqualSstNum(config::kL0_StopWritesTrigger)!= nullptr && versions_->current()->compaction_level_ == 0 ) {
       //There are too many level-0 files.
      cout<<"version l0 tnumber"<<versions_->NumLevelFiles(0) <<endl;
                  Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    }
    else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();

      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      delete log_;

      s = logfile_->Close();
      if (!s.ok()) {
        // We may have lost some data written to the previous log file.
        // Switch to the new log file anyway, but record as a background
        // error so we do not attempt any more writes.
        //
        // We could perhaps attempt to save the memtable corresponding
        // to log file and suppress the error if that works, but that
        // would add more complexity in a critical code path.
        RecordBackgroundError(s);
      }
      delete logfile_;

      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;    //lxw memtable to immutable
      // WriteBptree(imm_);
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();  //lxw:在此处不做immu的flush
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  request_tracker_->RecordWrite(key.ToString());
   //cout<<"insert: "<<key.ToString()<<endl;
  batch.Put(key, value);
  //修改
  //tree.normal_insertbptree(key,value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;
  //修改
  //tree=new BPlusTree(3);
  auto start_time = std::chrono::high_resolution_clock::now();
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
      partition->Deserialize(dbname + "/" + "partition_list.txt");
      cout<<"linklist number"<<partition->Print_toalnum()<<endl;
      HotTree_manager->Deserialize(dbname + "/" + "HotTree_list.txt");
      uint64_t fileSize = 0;
/*      options.env->GetFileSize(dbname + "/" + "ValidKey.txt", &fileSize);
      if (options.env->FileExists(dbname + "/" + "ValidKey.txt") &&  fileSize > 0) {
        ValidKey->Deserialize(dbname + "/" + "ValidKey.txt");
      }*/
      impl->tree->loadFromFile(dbname + "/" + "BPTree.txt");
      //impl->tree->print();
      HotTree_manager->TraverseSSTables([](uint64_t sst_number, const DBImpl::SSTableInfo& sst_info) {
        std::cout << "SSTable Number: " << sst_number
                  << ", Size: " << sst_info.size
                  << ", Min Key: " << sst_info.min_key
                  << ", Max Key: " << sst_info.max_key
                  << std::endl;});

      if(partition->IsEmpty()){
        for (int i = 0; i <= 99; ++i) {
          std::stringstream ss;
          ss << std::setw(2) << std::setfill('0') << i;
          std::string key = "user" + ss.str();
          while (key.length() < 24) {
            key += "0";
          }
          partition->Insert(key);
        }
        partition->Insert("user99999999999999999999");
        partition->Insert("v");

      }
      cout<<"new"<<endl;
      partition->Print_num();
      impl->partition_maxkey=partition->FindFirstLargerOrEqualKey("")->data;//
      //cout<<"line 1800: "<<impl->partition_maxkey<<endl;
      //partition->Print();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
  std::cout << "DB::Open()执行时间：" << duration.count() << "秒" << std::endl;
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
