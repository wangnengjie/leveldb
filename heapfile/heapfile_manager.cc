#include "heapfile_manager.h"

#include "db/filename.h"
#include "db/version_set.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <utility>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/status.h"

#include "util/io_uring.h"

#include "heapfile/heapfile.h"
#include "heapfile/heapfile_value.h"

namespace leveldb {

HeapFileManager::HeapFileManager(const std::string& dbname, Env* env,
                                 const Options& options)
    : dbname_(dbname),
      env_(env),
      options_(options),
      next_txn_id_(1),
      next_hf_number_(1) {
  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (auto& filename : filenames) {
    if (ParseFileName(filename, &number, &type) && type == kHeapFile) {
      next_hf_number_ = std::max(next_hf_number_, number + 1);
      AddHeapFile(number);
    }
  }
  SortHeapFile();
}

HeapFileManager::~HeapFileManager() {
  // TODO: for compact txn check
}

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

auto HeapFileManager::AddNewHeapFile() -> Status {
  uint64_t fn = NextHFNumber();
  return AddHeapFile(fn);
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

auto HeapFileManager::ReadHFValue(const HFValueMeta& meta, uint8_t* buf,
                                  HandleWrapper& h) -> Status {
  const HeapFile* hf = GetHeapFile(meta.file_number);
  if (hf == nullptr) {
    return Status::NotFound("HeapFile not found");
  }
  h = IoUring::Instance().DoIoReq(
      IoReq(false, hf->Fd(), meta.block_count * kHeapBlockSize,
            meta.start_index * kHeapBlockSize, buf));
  return Status::OK();
}

auto HeapFileManager::ReadHFValueSync(const HFValueMeta& meta, uint8_t* buf)
    -> Status {
  const HeapFile* hf = GetHeapFile(meta.file_number);
  if (hf == nullptr) {
    return Status::NotFound("HeapFile not found");
  }
  return IoUring::Instance().DoIoReqSync(
      IoReq(false, hf->Fd(), meta.block_count * kHeapBlockSize,
            meta.start_index * kHeapBlockSize, buf));
}

auto HeapFileManager::ReadDecodedValueSync(const HFValueMeta& meta,
                                           std::string& value) -> Status {
  Status s;
  uint8_t* hf_buf = nullptr;
  int ret = ::posix_memalign(reinterpret_cast<void**>(&hf_buf), kHeapBlockSize,
                             meta.block_count * kHeapBlockSize);
  assert(ret == 0);
  s = ReadHFValueSync(meta, hf_buf);
  if (!s.ok()) {
    delete hf_buf;
    return s;
  }
  size_t size = 0;
  s = GetHFValueDecodeLength(hf_buf, &size);
  if (!s.ok()) {
    delete hf_buf;
    return s;
  }
  value.resize(size);
  s = DecodeHFValue(hf_buf, meta.block_count * kHeapBlockSize,
                    reinterpret_cast<uint8_t*>(value.data()));
  delete hf_buf;
  return s;
}

auto HeapFileManager::SortHeapFile() -> void {
  std::lock_guard<std::mutex> l(vec_mu_);
  std::sort(hfs_vec_.begin(), hfs_vec_.end(),
            [](const HeapFile* a, const HeapFile* b) {
              return a->FreeBlockCount() > b->FreeBlockCount();
            });
}

auto HeapFileManager::NewFlushTxn() -> FlushTxn { return FlushTxn(this); }

auto HeapFileManager::NewCompactTxn(const Compaction* compaction)
    -> CompactTxn {
  return CompactTxn(this, compaction);
}

auto HeapFileManager::AddPendingCompactTxn(CompactTxn&& txn) -> void {
  std::lock_guard<std::mutex> l(compact_mu_);
  pending_compact_txns_.push_back(std::move(txn));
}

auto HeapFileManager::CheckCompactTxnDone(const std::set<uint64_t>& live)
    -> void {
  std::lock_guard<std::mutex> l(compact_mu_);
  for (auto it = pending_compact_txns_.begin();
       it != pending_compact_txns_.end();) {
    bool to_commit = true;
    for (const auto& fn : it->RelateFiles()) {
      if (live.find(fn) != live.end()) {
        to_commit = false;
        break;
      }
    }
    if (to_commit) {
      it->Commit();
      it = pending_compact_txns_.erase(it);
    } else {
      ++it;
    }
  }
}

auto HeapFileManager::NextTxnId() -> uint64_t {
  std::lock_guard<std::mutex> l(mu_);
  return next_txn_id_++;
}

auto HeapFileManager::NextHFNumber() -> uint64_t {
  std::lock_guard<std::mutex> l(mu_);
  return next_hf_number_++;
}

using FlushTxn = HeapFileManager::FlushTxn;

FlushTxn::FlushTxn(HeapFileManager* hfm)
    : txn_id_(hfm->NextTxnId()),
      hfm_(hfm),
      pinned_hf_(nullptr),
      io_count_(0),
      status_(TxnStatus::kRunning) {
  submit_handles_.resize(kIoUringDepth);
  io_bufs_.resize(kIoUringDepth, IoBuf{.size = 0, .buf = nullptr});
}

auto FlushTxn::Add(const Slice& s, HFValueMeta& value_meta) -> Status {
  if (status_ != TxnStatus::kRunning) {
    return Status::HFError("FlushTxn is not running");
  }

  Status status;
  size_t cur_index = io_count_ % kIoUringDepth;

  if (*submit_handles_[cur_index] != nullptr) {
    submit_handles_[cur_index]->Wait();
  }

  size_t max_len = HFValueMaxEncodedLength(hfm_->options_, s.size());
  max_len = round_up(max_len);

  status = PrepareBuf(cur_index, max_len);
  if (!status.ok()) {
    return status;
  }

  size_t output_size;
  EncodeHFValue(hfm_->options_, s, io_bufs_[cur_index].buf, &output_size);
  output_size = round_up(output_size);
  assert(output_size == 4096);
  Mutation m = FindSpace(output_size);
  while (m.Offset() == kInvalidOffset) {
    status = hfm_->AddNewHeapFile();
    if (!status.ok()) {
      return status;
    }
    hfm_->SortHeapFile();
    m = FindSpace(output_size);
  }

  submit_handles_[cur_index] = std::move(
      IoUring::Instance().DoIoReq(IoReq(true, pinned_hf_->Fd(), output_size,
                                        m.Offset(), io_bufs_[cur_index].buf)));

  mutations_[pinned_hf_->FileNumber()].push_back(m);

  value_meta.file_number = pinned_hf_->FileNumber();
  value_meta.start_index = m.StartIndex();
  value_meta.block_count = m.BlockCount();

  io_count_++;
  return status;
}

auto FlushTxn::Commit() -> Status {
  if (status_ != TxnStatus::kRunning) {
    return Status::HFError("FlushTxn is not running");
  }
  Status s;

  for (auto& hw : submit_handles_) {
    if (*hw == nullptr) {
      continue;
    }
    hw->Wait();
    if (!hw->status().ok()) {
      return hw->status();
    }
  }

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
  Status s;
  for (auto& hw : submit_handles_) {
    if (*hw == nullptr) {
      continue;
    }
    hw->Wait();
    if (!hw->status().ok()) {
      s = hw->status();
    }
  }
  for (auto& [file_number, mutations] : mutations_) {
    HeapFile* hf = hfm_->GetHeapFile(file_number);
    assert(hf != nullptr);
    s = hf->UnSetBitmapAndFlush(mutations);
  }
  status_ = TxnStatus::kAborted;
  hfm_->SortHeapFile();
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

CompactTxn::CompactTxn(HeapFileManager* hfm, const Compaction* compaction)
    : txn_id_(hfm->NextTxnId()), hfm_(hfm), status_(TxnStatus::kRunning) {
  int files;
  files = compaction->num_input_files(0);
  for (int i = 0; i < files; i++) {
    relate_files_.push_back(compaction->input(0, i)->number);
  }
  files = compaction->num_input_files(1);
  for (int i = 0; i < files; i++) {
    relate_files_.push_back(compaction->input(1, i)->number);
  }
}

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