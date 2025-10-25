// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"
//#include"db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include <iostream>

using namespace std;

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}


Status BuildTablefrombptree(const std::string& dbname, Env* env, const Options& options,TableCache* table_cache, Iterator* iter, FileMetaData* meta,
                            uint64_t tmpaverage_optime,vector<string> *deletekey,LinkedList* partition,std::string *partition_maxkey,std::string* fs_key,bool &isresetting_fs_key) {
  cout<<"BuildTablefrombptree调用"<<endl;
  Status s;
  int stop_size=2097152;
  int count_size=0;
  meta->file_size = 0;
  BPlusTreeIterator* tmpbpt_iter = static_cast<BPlusTreeIterator*>(iter);
  //size_t now_linksize=partition->size_;
  cout<<"iter->Seek(Slice(fs_key))调用时的fs_key: "<<*fs_key<<endl;
  tmpbpt_iter->Seek_skiphotindex(Slice(*fs_key),isresetting_fs_key);

  cout<<"iter->Seek(Slice(fs_key))找到的迭代器起始key： "<<tmpbpt_iter->key().ToString()<<endl;

  *partition_maxkey = partition->FindFirstLargerKey(tmpbpt_iter->key().ToString())->data;
  //iter->SeekToFirst();
  std::string fname = TableFileName(dbname, meta->number);
  if (!tmpbpt_iter->Valid()) {
    cout<<"line100迭代器失效，重置fs_key和partition_maxkey"<<endl;
    *fs_key="";
    tmpbpt_iter->SeekToFirst_skiphotindex();
    *partition_maxkey = partition->FindFirstLargerOrEqualKey(tmpbpt_iter->key().ToString())->data;
    if(partition->FindFirstLargerOrEqualKey(tmpbpt_iter->key().ToString()) ==nullptr )cout<<"line104空指针 "<<endl;
    cout<<"line104迭代器失效后，迭代器的key ： "<< tmpbpt_iter->key().ToString()<<endl;
    cout<<"line104迭代器失效后，重置partition_maxkey ： "<< *partition_maxkey<<endl;
  };
  if (tmpbpt_iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    //EncodeTo() 和 DecodeFrom() 方法分别用于将 VersionEdit 序列化为 manifest 记录和从 MANIFEST 记录中反序列化出 VersionEdit
    meta->smallest.DecodeFrom(tmpbpt_iter->key());
    cout<<"smallest"<<tmpbpt_iter->key().ToString()<<endl;
    Slice key;
    string tmpkey;
    for (; tmpbpt_iter->Valid() && count_size<=stop_size ; tmpbpt_iter->FlushNext()) {
      key = tmpbpt_iter->key();
      if( tmpbpt_iter->last_optime() <= tmpaverage_optime) {
        if(partition_maxkey->compare(key.ToString()) <0) {
           //cout<<"原始："<<*partition_maxkey<<endl;
          if(*partition_maxkey == partition->max_data) {
            cout<<"*partition_maxkey == partition->max_data"<<endl;
            continue;
          }else {
            *fs_key = tmpbpt_iter->keystring();
            cout<<"builder 128 大于partition_maxkey的key: "<<key.ToString()<<endl;
            *partition_maxkey = partition->FindFirstLargerOrEqualKey(key.ToString())->data;
            cout<<"fs_key："<< *fs_key<<endl;
            cout<<"跳出for循环 新的partition_maxkey: "<<*partition_maxkey<<endl;
            //iter->Prev();
            //key = iter->key();
           // cout<<"执行完iter->Prev();的key: "<<key.ToString().substr(0,10)<<endl;
            //if(meta->smallest.user_key() == iter->key()) return Status::Corruption("Buildtablefrombptree is empty.");
            break;

          }
        }
      tmpkey=key.ToString();
      builder->Add(key, tmpbpt_iter->value());
      deletekey->push_back(tmpbpt_iter->keystring());
      count_size+=key.size()+tmpbpt_iter->value().size();
      //cout<<tmpbpt_iter->keystring()<<endl;

      }
    }

    //if(!iter->Valid()) cout<<"!iter->Valid() break"<<endl;
    //if(count_size > stop_size) cout<<"stop_size break"<<endl;
     //增加SSTNUM计数
    if (!tmpkey.empty()) {
      meta->largest.DecodeFrom(Slice(tmpkey));
     cout<<"largest: "<<tmpkey.substr(0,10)<<endl;
      /*   cout<<"跳出for循环后的partition_maxkey: "<<*partition_maxkey<<endl;
        partition->FindFirstLargerOrEqualKey(tmpkey)->sst_num++;
        cout<<"分区节点： "<<partition->FindFirstLargerOrEqualKey(tmpkey)->data<<" num: "<<partition->FindFirstLargerOrEqualKey(tmpkey)->sst_num<<endl;*/
    }else{
      meta->largest = meta->smallest;
    }

    // Finish and check for builder errors
    s = builder->Finish();
   // cout << " builder status:" << s.ToString() << endl;
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
      const uint64_t file_size = builder->FileSize();
      meta->file_size = file_size;
}
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      //cout << " builder status:" << s.ToString() << endl;
      delete it;
    }
  }

  // Check for input iterator errors
 /* if (!iter->status().ok()) {
    s = iter->status();
  }*/

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
    //cout<<"bulid remove"<<endl;
  }
  return s;
}

Status BuildTablefromHotTree(const std::string& dbname, Env* env, const Options& options,TableCache* table_cache, BPlusTreeIterator*  iter, FileMetaData* meta,vector<string>* deletekey,
                           std::string &minkey,std::string &maxkey,LinkedList* partition) {
  Status s;
  meta->file_size = 0;
  int count_size=0;

  std::string fname = TableFileName(dbname, meta->number);
  if (!iter->Valid()) {

    return Status::IOError("No Hot Key in BPtree");

  };
  if (iter->Valid()) {
   // string this_maxkey = partition->FindFirstLargerOrEqualKey(iter->key().ToString())->data;
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    //EncodeTo() 和 DecodeFrom() 方法分别用于将 VersionEdit 序列化为 manifest 记录和从 MANIFEST 记录中反序列化出 VersionEdit
    meta->smallest.DecodeFrom(iter->key());

    Slice key;
    string tmpkey;
   // BPlusTreeIterator* tmpbpt_iter = static_cast<BPlusTreeIterator*>(iter);

      for (; iter->Valid()   ; iter->H_Next()) {
        key = iter->key();
        tmpkey=key.ToString();
        builder->Add(key, iter->value());
        deletekey->push_back(iter->keystring());
       }


    if (!tmpkey.empty()) {
      meta->largest.DecodeFrom(Slice(tmpkey));
        }

    // Finish and check for builder errors
    s = builder->Finish();
    // cout << " builder status:" << s.ToString() << endl;
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
      const uint64_t file_size = builder->FileSize();
      meta->file_size = file_size;
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      //cout << " builder status:" << s.ToString() << endl;
      delete it;
    }
  }

  // Check for input iterator errors
  /* if (!iter->status().ok()) {
     s = iter->status();
   }*/

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
    //cout<<"bulid remove"<<endl;
  }
  return s;
}


}  // namespace leveldb
