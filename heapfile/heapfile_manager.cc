#include "heapfile_manager.h"

#include "db/filename.h"

#include "leveldb/status.h"

#include "util/io_uring.h"

#include "heapfile/heapfile.h"

namespace leveldb {

HeapFileManager::HeapFileManager(const std::string& dbname, Env* env,
                                 const Options& options)
    : dbname_(dbname), env_(env), options_(options), next_txn_id_(0) {}

auto HeapFileManager::AddHeapFile(uint64_t file_number) -> Status {
  std::lock_guard<std::mutex> l(mu_);
  if (hfs_map_.find(file_number) != hfs_map_.end()) {
    return Status::OK();
  }
  std::string fname = HeapFileName(dbname_, file_number);
  std::unique_ptr<HeapFile> hf = nullptr;
  Status s = HeapFile::Open(&options_, fname, &hf);
  if (!s.ok()) {
    return s;
  }
  hfs_vec_.push_back(hf.get());
  hfs_map_.emplace(file_number, std::move(hf));
  return s;
}

auto HeapFileManager::GetHeapFile(uint64_t file_number) const
    -> const HeapFile* {
  std::lock_guard<std::mutex> l(mu_);
  auto it = hfs_map_.find(file_number);
  if (it == hfs_map_.end()) {
    return nullptr;
  }
  return it->second.get();
}

auto HeapFileManager::GetHeapFile(uint64_t file_number) -> HeapFile* {
  std::lock_guard<std::mutex> l(mu_);
  auto it = hfs_map_.find(file_number);
  if (it == hfs_map_.end()) {
    return nullptr;
  }
  return it->second.get();
}

auto HeapFileManager::ReadValue(uint64_t file_number, uint16_t start_index,
                                uint16_t block_count, uint8_t* buf,
                                HandleWrapper& hw) -> Status {
  const HeapFile* hf = nullptr;
  {
    std::lock_guard<std::mutex> l(mu_);
    hf = GetHeapFile(file_number);
  }
  if (hf == nullptr) {
    return Status::NotFound("HeapFile not found");
  }
  hw = std::move(IoUring::Instance().DoIoReq(
      IoReq(false, hf->Fd(), block_count * kHeapBlockSize,
            start_index * kHeapBlockSize, buf)));
  return Status::OK();
}

auto HeapFileManager::ReadValueSync(uint64_t file_number, uint16_t start_index,
                                    uint16_t block_count, uint8_t* buf)
    -> Status {
  const HeapFile* hf = nullptr;
  {
    std::lock_guard<std::mutex> l(mu_);
    hf = GetHeapFile(file_number);
  }
  if (hf == nullptr) {
    return Status::NotFound("HeapFile not found");
  }
  auto hw = IoUring::Instance().DoIoReq(
      IoReq(false, hf->Fd(), block_count * kHeapBlockSize,
            start_index * kHeapBlockSize, buf));
  hw.Get().Wait();
  return std::move(hw.Get().status());
}

auto HeapFileManager::SortHeapFile() -> void {
  std::lock_guard<std::mutex> l(mu_);
  std::sort(hfs_vec_.begin(), hfs_vec_.end(),
            [](const HeapFile* a, const HeapFile* b) {
              return a->FreeBlockCount() > b->FreeBlockCount();
            });
}

auto HeapFileManager::NewFlushTxn() -> FlushTxn { return FlushTxn(this); }

auto HeapFileManager::NewCompactTxn() -> CompactTxn { return CompactTxn(this); }

auto HeapFileManager::NextTxnId() -> uint64_t {
  std::lock_guard<std::mutex> l(mu_);
  return next_txn_id_++;
}

using FlushTxn = HeapFileManager::FlushTxn;

FlushTxn::FlushTxn(HeapFileManager* hfm)
    : txn_id_(hfm->NextTxnId()),
      hfm_(hfm),
      pinned_hf_(nullptr),
      cur_batch_io_count_(0),
      batch_count_(kIoUringDepth / 2),
      status_(TxnStatus::kRunning) {
  pending_batch_.resize(batch_count_, IoReq(true, -1, 0, 0, nullptr));
  cur_batch_.resize(batch_count_, IoReq(true, -1, 0, 0, nullptr));

  submit_handles_.reserve(batch_count_);
}

FlushTxn::~FlushTxn() {
  if (status_ == TxnStatus::kRunning) {
    Abort();
  }
}

}  // namespace leveldb