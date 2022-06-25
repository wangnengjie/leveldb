#include "heapfile_manager.h"

#include "db/filename.h"
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <mutex>

#include "leveldb/status.h"

#include "util/io_uring.h"

#include "heapfile/heapfile.h"
#include "heapfile/heapfile_value.h"

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
  Status s = HeapFile::Open(&options_, dbname_, file_number, &hf);
  if (!s.ok()) {
    return s;
  }
  {
    std::lock_guard<std::mutex> l(vec_mu_);
    hfs_vec_.push_back(hf.get());
  }
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
  const HeapFile* hf = GetHeapFile(file_number);
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
  const HeapFile* hf = GetHeapFile(file_number);
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
  std::lock_guard<std::mutex> l(vec_mu_);
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
      io_count_(0),
      batch_size_(kIoUringDepth / 2),
      status_(TxnStatus::kRunning) {
  io_bufs_.resize(batch_size_ * 2, IoBuf{.size = 0, .buf = nullptr});
  submit_handles_.reserve(batch_size_);
}

auto FlushTxn::Add(Slice& s, HFValueMeta& value_meta) -> Status {
  if (status_ != TxnStatus::kRunning) {
    return Status::HFError("FlushTxn is not running");
  }

  Status status;
  size_t cur_buf_index = io_count_ % (batch_size_ * 2);
  size_t max_len = HFValueMaxEncodedLength(hfm_->options_, s.size());
  max_len = round_up(max_len);

  status = PrepareBuf(cur_buf_index, max_len);
  if (!status.ok()) {
    return status;
  }

  size_t output_size;
  EncodeHFValue(hfm_->options_, s, io_bufs_[cur_buf_index].buf, &output_size);
  output_size = round_up(output_size);
  Mutation m = FindSpace(output_size);
  if (m.Offset() == kInvalidOffset) {
    return Status::HFNoSpace();
  }

  batch_.emplace_back(true, pinned_hf_->Fd(), output_size, m.Offset(),
                      io_bufs_[cur_buf_index].buf);
  mutations_[pinned_hf_->FileNumber()].push_back(m);

  value_meta.file_number = pinned_hf_->FileNumber();
  value_meta.start_index = m.StartIndex();
  value_meta.block_count = m.BlockCount();

  // current batch is full, submit it
  // 1. wait for previous batch to finish
  // 2. submit current batch
  if (batch_.size() == batch_size_) {
    status = SubmitBatch();
  }
  io_count_++;
  return status;
}

auto FlushTxn::Commit() -> Status {
  if (status_ != TxnStatus::kRunning) {
    return Status::HFError("FlushTxn is not running");
  }
  Status s;
  if (batch_.size() > 0) {
    s = SubmitBatch();
    if (!s.ok()) {
      return s;
    }
  }
  s = WaitPrevBatch();
  if (!s.ok()) {
    return s;
  }
  assert(submit_handles_.size() == 0);
  for (auto& [file_number, mutations] : mutations_) {
    HeapFile* hf = hfm_->GetHeapFile(file_number);
    assert(hf != nullptr);
    s = hf->SetBitmapAndFlush(mutations);
    if (!s.ok()) {
      return s;
    }
  }
  assert(s.ok());
  status_ = TxnStatus::kCommitted;
  hfm_->SortHeapFile();
  return s;
}

auto FlushTxn::Abort() -> Status {
  if (status_ != TxnStatus::kRunning) {
    return Status::HFError("FlushTxn is not running");
  }
  Status s = WaitPrevBatch();
  for (auto& [file_number, mutations] : mutations_) {
    HeapFile* hf = hfm_->GetHeapFile(file_number);
    assert(hf != nullptr);
    s = hf->UnSetBitmapAndFlush(mutations);
  }
  status_ = TxnStatus::kAborted;
  hfm_->SortHeapFile();
  return s;
}

auto FlushTxn::SubmitBatch() -> Status {
  Status s = WaitPrevBatch();
  submit_handles_ = IoUring::Instance().BatchIoReq(batch_);
  batch_.clear();
  return s;
}

auto FlushTxn::FindSpace(size_t size) -> Mutation {
  Mutation m;
  size_t block_count = size / kHeapBlockSize;

  if (pinned_hf_ != nullptr) {
    m = pinned_hf_->AllocBlocks(size);
    if (m.Offset() != kInvalidOffset) {
      return m;
    }
    pinned_hf_ = nullptr;
  }
  hfm_->SortHeapFile();
  {
    // TODO: use a iter func provide by hfm_->hfs_vec_
    std::lock_guard<std::mutex> l(hfm_->vec_mu_);
    for (auto& hf : hfm_->hfs_vec_) {
      if (hf->FreeBlockCount() < block_count) {
        break;
      }
      m = hf->AllocBlocks(size);
      if (m.Offset() != kInvalidOffset) {
        pinned_hf_ = hf;
        return m;
      }
    }
  }
  return m;
}

auto FlushTxn::WaitPrevBatch() -> Status {
  Status s;
  for (auto& h : submit_handles_) {
    h.Get().Wait();
    if (!h.Get().status().ok()) {
      s = h.Get().status();
    }
  }
  submit_handles_.clear();
  return s;
}

auto FlushTxn::PrepareBuf(size_t index, size_t size) -> Status {
  if (io_bufs_[index].size < size) {
    delete[] io_bufs_[index].buf;
    int ret = ::posix_memalign(reinterpret_cast<void**>(&io_bufs_[index].buf),
                               kHeapBlockSize, size);
    if (ret != 0) {
      return Status::IOError("posix_memalign failed");
    }
    io_bufs_[index].size = size;
  }
  return Status::OK();
}

FlushTxn::~FlushTxn() {
  if (status_ == TxnStatus::kRunning) {
    Abort();
  }
  for (auto& buf : io_bufs_) {
    delete[] buf.buf;
  }
}

using CompactTxn = HeapFileManager::CompactTxn;

CompactTxn::CompactTxn(HeapFileManager* hfm)
    : txn_id_(hfm->NextTxnId()), hfm_(hfm), status_(TxnStatus::kRunning) {}

CompactTxn::~CompactTxn() {
  if (status_ == TxnStatus::kRunning) {
    Abort();
  }
}

auto CompactTxn::Remove(const HFValueMeta& value_meta) -> void {
  mutations_[value_meta.file_number].push_back(
      Mutation(value_meta.start_index, value_meta.block_count));
}

auto CompactTxn::Commit() -> Status {
  Status s;
  for (auto& [file_number, mutations] : mutations_) {
    HeapFile* hf = hfm_->GetHeapFile(file_number);
    assert(hf != nullptr);
    s = hf->UnSetBitmapAndFlush(mutations);
    if (!s.ok()) {
      return s;
    }
  }
  status_ = TxnStatus::kCommitted;
  hfm_->SortHeapFile();
  return s;
}

auto CompactTxn::Abort() -> Status {
  // TODO: we need to rollback released blocks
  // TODO: but how to do so? may be rebuild heapfile then?
  status_ = TxnStatus::kAborted;
  return Status::OK();
}

}  // namespace leveldb