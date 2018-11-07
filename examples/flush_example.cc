//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <chrono>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <time.h>
#include <unistd.h>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"

namespace rocksdb {

std::string getKey(int i) {
    std::stringstream ss;
    ss << "key" << i;

    return ss.str();
}

void dbput(DB *db, int i, int j, ColumnFamilyHandle *h, bool sync_write) {
    Status s;
    WriteOptions wopt;
    wopt.sync = sync_write;
    wopt.disableWAL = false;
    for(int x=i; x < j; x++) {
        s = db->Put(wopt, h, getKey(x), getKey(x)/*serialize(m)*/);
        assert(s.ok());
    }
}

} // namespace rocksdb

/*
 * function requires the following arguments
 * 1. DB path
 * 2. Total number of key-value pairs
 */
int main(int argc, char *argv[]) {

  if(argc != 4){
    std::cout<< "The program requires 2 arguments." << std::endl
        << "1. Path to the db" << std::endl
        << "2. Total number of key-value pair to be inserted" << std::endl
        << "3. 0 if non-sync write, non-zero otherwise" << std::endl;
    return 1;
  }

  std::string dbPath(argv[1]);
  std::stringstream ss;
  ss << argv[2];
  unsigned int totalKeyValuePairs;
  ss >> totalKeyValuePairs;

  unsigned int sync_write;
  ss.clear();
  ss << argv[3];
  ss >> sync_write;

  std::cout << "Total key-value pair: " << totalKeyValuePairs << std::endl;
  std::cout << "Use sync write: " << sync_write << std::endl;

  rocksdb::DB* db = nullptr;
  rocksdb::Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  options.write_buffer_size= 1 * 1024 * 1024LL;

  // open DB
  rocksdb::Status s = rocksdb::DB::Open(options, dbPath, &db);
  assert(s.ok());

  // create cf
  rocksdb::ColumnFamilyHandle *h;
  s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "local", &h);
  assert(s.ok());

  // insert some data
  dbput(std::ref(db), 0, totalKeyValuePairs/2, h, sync_write != 0);

  // fork
  pid_t result = fork();
  if(result) {
    if(result < 0) {
        _exit(1);
    }
    _exit(0);
  }

  // drop column family
  s = db->DropColumnFamily(h);
  assert(s.ok());
  s = db->DestroyColumnFamilyHandle(h);
  assert(s.ok());

  // create column family again
  s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "local", &h);
  assert(s.ok());

  // insert some more data
  dbput(std::ref(db), totalKeyValuePairs/2, totalKeyValuePairs, h, sync_write != 0);

  // flush
  s = db->Flush(rocksdb::FlushOptions(), h);
  assert(s.ok());

  delete db;

  return 0;
}
