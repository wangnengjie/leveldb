#ifndef STORAGE_LEVELDB_HEAPFILE_MANAGER_H_
#define STORAGE_LEVELDB_HEAPFILE_MANAGER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "leveldb/options.h"
#include "leveldb/status.h"

#include "util/coding.h"
#include "util/io_uring.h"

#include "heapfile/heapfile.h"
#include "heapfile/heapfile_value.h"

namespace leveldb {

class HeapFileManager {
 private:
  const std::string dbname_;
  Env* const env_;
  const Options& options_;
  mutable std::mutex mu_;
  std::unordered_map<uint64_t, std::unique_ptr<HeapFile>> hfs_map_;
  mutable std::mutex vec_mu_;
  std::vector<HeapFile*> hfs_vec_;
  uint64_t next_txn_id_;

 public:
  // FlushTxn collect all large value to store to heapfile when immutable table
  // flush
  class FlushTxn;
  // CompactTxn collect all large value to remove from heapfile during
  // compaction
  class CompactTxn;
  friend class FlushTxn;
  friend class CompactTxn;

 public:
  HeapFileManager(const std::string& dbname, Env* env, const Options& options);
  auto AddHeapFile(uint64_t file_number) -> Status;
  auto GetHeapFile(uint64_t file_number) const -> const HeapFile*;
  auto ReadValue(uint64_t file_number, uint16_t start_index,
                 uint16_t block_count, uint8_t* buf, HandleWrapper& hw)
      -> Status;
  auto ReadValueSync(uint64_t file_number, uint16_t start_index,
                     uint16_t block_count, uint8_t* buf) -> Status;

  // sort heap file by free block count. this help the alloc to find
  // free block more faster.
  auto SortHeapFile() -> void;

  auto NewFlushTxn() -> FlushTxn;

  auto NewCompactTxn() -> CompactTxn;

 private:
  auto GetHeapFile(uint64_t file_number) -> HeapFile*;
  auto NextTxnId() -> uint64_t;
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
  std::unordered_map<uint64_t, std::vector<Mutation>> mutations_;
  HeapFile* pinned_hf_;
  std::vector<HandleWrapper> submit_handles_;
  std::vector<IoReq> batch_;
  std::vector<IoBuf> io_bufs_;
  uint64_t io_count_;
  const uint32_t batch_size_;
  TxnStatus status_;

 public:
  FlushTxn(HeapFileManager* hfm);
  ~FlushTxn();
  auto Add(Slice& s, HFValueMeta& value_meta) -> Status;
  auto Commit() -> Status;
  auto Abort() -> Status;
  // auto EncodeMutation(std::string& s) const -> void;

 private:
  auto SubmitBatch() -> Status;
  // caller make sure the size is kHeapBlockSize aligned
  auto FindSpace(size_t size) -> Mutation;
  auto WaitPrevBatch() -> Status;
  // caller make sure the size is kHeapBlockSize aligned
  auto PrepareBuf(size_t index, size_t size) -> Status;
};

class HeapFileManager::CompactTxn {
 private:
  uint64_t txn_id_;
  HeapFileManager* hfm_;
  std::unordered_map<uint64_t, std::vector<Mutation>> mutations_;
  TxnStatus status_;

 public:
  CompactTxn(HeapFileManager* hfm);
  ~CompactTxn();
  auto Remove(const HFValueMeta& value_meta) -> void;
  auto Commit() -> Status;
  auto Abort() -> Status;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_HEAPFILE_MANAGER_H_