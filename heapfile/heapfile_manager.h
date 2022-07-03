#ifndef STORAGE_LEVELDB_HEAPFILE_MANAGER_H_
#define STORAGE_LEVELDB_HEAPFILE_MANAGER_H_

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "leveldb/options.h"
#include "leveldb/status.h"

#include "util/coding.h"
#include "util/io_uring.h"

#include "heapfile/heapfile.h"
#include "heapfile/heapfile_value.h"

namespace leveldb {

class Compaction;

class HeapFileManager {
 public:
  // FlushTxn collect all large value to store to heapfile when immutable table
  // flush
  class FlushTxn;
  // CompactTxn collect all large value to remove from heapfile during
  // compaction
  class CompactTxn;
  friend class FlushTxn;
  friend class CompactTxn;

 private:
  const std::string dbname_;
  Env* const env_;
  const Options& options_;
  mutable std::mutex mu_;
  uint64_t next_txn_id_;
  uint64_t next_hf_number_;
  std::map<uint64_t, std::unique_ptr<HeapFile>> hfs_map_;

  mutable std::mutex compact_mu_;
  std::list<CompactTxn> pending_compact_txns_;

  mutable std::mutex vec_mu_;
  std::vector<HeapFile*> hfs_vec_;

 public:
  HeapFileManager(const std::string& dbname, Env* env, const Options& options);
  ~HeapFileManager();
  auto GetHeapFile(uint64_t file_number) const -> const HeapFile*;
  auto ReadHFValue(const HFValueMeta& meta, uint8_t* buf, HandleWrapper& h)
      -> Status;
  auto ReadHFValueSync(const HFValueMeta& meta, uint8_t* buf) -> Status;
  auto ReadDecodedValueSync(const HFValueMeta& meta, std::string& value)
      -> Status;

  // sort heap file by free block count. this help the alloc to find
  // free block more faster.
  auto SortHeapFile() -> void;

  auto NewFlushTxn() -> FlushTxn;

  auto NewCompactTxn(const Compaction* compaction) -> CompactTxn;

  auto AddPendingCompactTxn(CompactTxn&& txn) -> void;

  auto CheckCompactTxnDone(const std::set<uint64_t>& live) -> void;

 private:
  auto AddHeapFile(uint64_t file_number) -> Status;
  auto AddNewHeapFile() -> Status;
  auto GetHeapFile(uint64_t file_number) -> HeapFile*;
  auto NextTxnId() -> uint64_t;
  auto NextHFNumber() -> uint64_t;
};

using Mutation = HeapFile::Mutation;

enum class TxnStatus : uint8_t {
  kRunning,
  kCommitted,
  kAborted,
};

class HeapFileManager::FlushTxn {
 private:
  struct IoBuf {
    size_t size;
    uint8_t* buf;  // aligned memory
  };

 private:
  uint64_t txn_id_;
  HeapFileManager* hfm_;
  std::map<uint64_t, std::vector<Mutation>> mutations_;
  HeapFile* pinned_hf_;
  std::vector<HandleWrapper> submit_handles_;
  std::vector<IoBuf> io_bufs_;
  uint64_t io_count_;
  TxnStatus status_;

 public:
  FlushTxn(HeapFileManager* hfm);
  ~FlushTxn();
  auto Add(const Slice& s, HFValueMeta& value_meta) -> Status;
  auto Commit() -> Status;
  auto Abort() -> Status;
  // auto EncodeMutation(std::string& s) const -> void;

 private:
  // caller make sure the size is kHeapBlockSize aligned
  auto FindSpace(size_t size) -> Mutation;
  // caller make sure the size is kHeapBlockSize aligned
  auto PrepareBuf(size_t index, size_t size) -> Status;
};

class HeapFileManager::CompactTxn {
 private:
  uint64_t txn_id_;
  HeapFileManager* hfm_;
  std::vector<uint64_t> relate_files_;
  std::map<uint64_t, std::vector<Mutation>> mutations_;
  TxnStatus status_;

 public:
  friend class HeapFileManager;

  CompactTxn(HeapFileManager* hfm, const Compaction* compaction);
  CompactTxn(CompactTxn&& txn) = default;
  ~CompactTxn();
  auto Remove(const HFValueMeta& value_meta) -> void;
  auto Abort() -> Status;
  auto RelateFiles() const -> const std::vector<uint64_t>& {
    return relate_files_;
  };

 private:
  auto Commit() -> Status;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_HEAPFILE_MANAGER_H_