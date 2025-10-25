// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "leveldb/status.h"
#include "bptree.h"
#include "linklist.h"
namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta);
Status BuildTablefrombptree(const std::string& dbname, Env* env, const Options& options,TableCache* table_cache,Iterator* iter, FileMetaData* meta,
                            uint64_t tmpaverage_optime,vector<string> *deletekey,LinkedList* partition,std::string* partition_maxkey,std::string* fs_key,bool &isresetting_fs_key);
Status BuildTablefromHotTree(const std::string& dbname, Env* env, const Options& options,TableCache* table_cache, BPlusTreeIterator* iter, FileMetaData* meta,vector<string>* deletekey,
                             std::string &minkey,std::string &maxkey,LinkedList* partition);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
