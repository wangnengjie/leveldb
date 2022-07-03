// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include <cassert>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "heapfile/heapfile_manager.h"
#include "heapfile/heapfile_value.h"

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, HeapFileManager* heapfile_manager,
                  Iterator* iter, FileMetaData* meta, bool is_flush) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  HeapFileManager::FlushTxn txn = heapfile_manager->NewFlushTxn();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    InternalKey k;
    ParsedInternalKey ikey;
    HFValueMeta value_meta;
    std::string hf_value;

    TableBuilder* builder = new TableBuilder(options, file);

    Slice key;
    for (size_t count = 0; iter->Valid(); iter->Next()) {
      if (is_flush &&
          iter->value().size() >= options.separate_value_threshold) {
        ParseInternalKey(iter->key(), &ikey);
        ikey.type = kTypeHFValue;
        k.SetFrom(ikey);
        key = k.Encode();
        s = txn.Add(iter->value(), value_meta);
        assert(s.ok());  //! just for debug, will remove later
        // if (!s.ok()) {
        //   txn.Abort();
        // }
        value_meta.EncodeTo(hf_value);
        builder->Add(key, Slice(hf_value));
        hf_value.clear();
      } else {
        key = iter->key();
        builder->Add(key, iter->value());
      }
      count++;
      if (count == 1) {
        meta->smallest.DecodeFrom(key);
      }
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
    s = txn.Commit();
    if (!s.ok()) {
      txn.Abort();
      env->RemoveFile(fname);
    }
  } else {
    txn.Abort();
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
