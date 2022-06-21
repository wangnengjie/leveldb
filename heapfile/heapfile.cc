#include "heapfile.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <mutex>

#include "leveldb/cache.h"
#include "leveldb/env.h"
#include "leveldb/status.h"

#include "util/io_uring.h"

namespace leveldb {
auto HeapFile::Open(const Options* options, std::string filename,
                    std::unique_ptr<HeapFile>* heap_file) -> Status {
  int fd = -1;
  int ret = 0;
  bool exist = true;
  if (!options->env->FileExists(filename)) {
    exist = false;
    fd = ::open(filename.c_str(), O_CREAT | O_RDWR | O_DIRECT | O_SYNC, 0644);
    ret = ::fallocate(fd, 0, 0, kHeapFileSize);
  } else {
    fd = ::open(filename.c_str(), O_RDWR | O_DIRECT | O_SYNC);
  }

  if (fd < 0) {
    return Status::IOError(filename, strerror(errno));
  }
  if (ret < 0) {
    return Status::IOError(filename, strerror(errno));
  }

  uint64_t cache_id = 0;
  if (options->block_cache != nullptr) {
    cache_id = options->block_cache->NewId();
  }
  SuperBlock* super_block = nullptr;
  ret = ::posix_memalign(reinterpret_cast<void**>(&super_block), kHeapBlockSize,
                         kHeapBlockSize);
  assert(ret == 0);
  {
    auto hw = IoUring::Instance().DoIoReq(
        IoReq(false, fd, kHeapBlockSize, kSuperBlockOffset,
              reinterpret_cast<uint8_t*>(super_block)));
  }

  if (exist) {
    if (!super_block->VerifyCheckSum()) {
      return Status::Corruption("heap file checksum error");
    }
  } else {
    for (int i = 0; i < kBitmapSize; i++) {
      super_block->bitmap[i] = 0;
    }
    super_block->SetCheckSum();
    auto hw = IoUring::Instance().DoIoReq(
        IoReq(true, fd, kHeapBlockSize, kSuperBlockOffset,
              reinterpret_cast<uint8_t*>(super_block)));
  }

  *heap_file = std::unique_ptr<HeapFile>(
      new HeapFile(filename, fd, cache_id, super_block));

  return Status::OK();
}

HeapFile::HeapFile(std::string filename, int fd, uint64_t cache_id,
                   SuperBlock* super_block)
    : filename_(filename),
      fd_(fd),
      cache_id_(cache_id),
      free_block_count_(0),
      super_block_(super_block) {
  InitFreeBlockList();
}

HeapFile::~HeapFile() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
  if (super_block_ != nullptr) {
    delete super_block_;
  }
  for (size_t i = 0; i < kFreeListGroupSize; i++) {
    while (dummys_[i].next != nullptr) {
      auto to_free = dummys_[i].next;
      dummys_[i].next = to_free->next;
      delete to_free;
    }
  }
}

auto HeapFile::AllocBlocks(uint64_t nbytes) -> Mutation {
  Mutation m;
  uint64_t real_bytes = round_up(nbytes);
  uint32_t block_count = real_bytes / kHeapBlockSize;
  size_t free_list_index = CalcFreeListIndex(block_count);
  FreeNode* pos = nullptr;
  FreeNode* prev = nullptr;

  std::lock_guard<std::mutex> lk(mu_);

  for (size_t i = free_list_index; i < kFreeListGroupSize; i++) {
    prev = &dummys_[i];
    pos = dummys_[i].next;
    while (pos != nullptr) {
      if (pos->block_count >= block_count) {
        break;
      }
      prev = pos;
      pos = pos->next;
    }
    // if pos is nullptr, means no enough block in this group
    if (pos == nullptr) {
      continue;
    }
    // pick this node out from free list
    prev->next = pos->next;
    pos->next = nullptr;
    m.start_index = pos->start_index;
    m.block_count = block_count;
    // update this free node
    pos->block_count -= block_count;
    pos->start_index += block_count;
    if (pos->block_count == 0) {
      delete pos;
    } else {
      // find a list to insert this node
      auto dummy = &dummys_[CalcFreeListIndex(pos->block_count)];
      pos->next = dummy->next;
      dummy->next = pos;
    }
    break;
  }

  // if success to alloc blocks, update free block count
  if (m.start_index != kInvalidBitMapIndex) {
    free_block_count_ -= m.block_count;
  }

  return m;
}

auto HeapFile::ReleaseBlocks(const std::vector<Mutation>& mutations) -> void {
  std::lock_guard<std::mutex> lk(mu_);
  for (const auto& m : mutations) {
    ReleaseBlock(m);
  }
}

auto HeapFile::SetBitmapAndFlush(const std::vector<Mutation>& mutations)
    -> void {
  std::lock_guard<std::mutex> lk(mu_);
  for (auto& m : mutations) {
    for (int i = 0; i < m.block_count; i++) {
      super_block_->Set(m.start_index + i);
    }
  }
  super_block_->SetCheckSum();
  {
    auto hw = IoUring::Instance().DoIoReq(
        IoReq(true, fd_, kHeapBlockSize, kSuperBlockOffset,
              reinterpret_cast<uint8_t*>(super_block_)));
  }
}

auto HeapFile::UnSetBitmapAndFlush(const std::vector<Mutation>& mutations)
    -> void {
  std::lock_guard<std::mutex> lk(mu_);
  for (const auto& m : mutations) {
    for (int i = 0; i < m.block_count; i++) {
      super_block_->UnSet(m.start_index + i);
    }
    ReleaseBlock(m);
  }
  super_block_->SetCheckSum();
  {
    auto hw = IoUring::Instance().DoIoReq(
        IoReq(true, fd_, kHeapBlockSize, kSuperBlockOffset,
              reinterpret_cast<uint8_t*>(super_block_)));
  }
}

auto HeapFile::InitFreeBlockList() -> void {
  for (size_t i = 0; i < kFreeListGroupSize; i++) {
    dummys_[i].next = nullptr;
    dummys_[i].start_index = kInvalidBitMapIndex;
    dummys_[i].block_count = 0;
  }
  uint32_t start_index = kInvalidBitMapIndex;
  uint32_t block_count = 0;
  for (size_t i = 0; i < 8 * kBitmapSize; i++) {
    if (super_block_->At(i) == 0) {
      // this is a free block
      start_index = start_index == kInvalidBitMapIndex ? i : start_index;
      block_count++;
      free_block_count_++;
    } else {
      // we meet a used block
      if (start_index == kInvalidBitMapIndex) {
        continue;
      }
      // add sequential free blocks to free list
      auto node = new FreeNode(start_index, block_count);
      auto dummy = &dummys_[CalcFreeListIndex(block_count)];
      node->next = dummy->next;
      dummy->next = node;
      start_index = kInvalidBitMapIndex;
      block_count = 0;
    }
  }
}

// release related blocks to free list.
// 1. check if the start block is not the first free block.
// 2. check if the end block is not the last free block.
// 3. add to list.
// ! we can not use bitmap to check because bitmap will not update
// ! when allocate a free space util all alloc is submit by SetBitmapAndFlush.
auto HeapFile::ReleaseBlock(const Mutation& mutation) -> void {
  // update free block count
  free_block_count_ += mutation.block_count;

  FreeNode* prev_free = nullptr;
  FreeNode* next_free = nullptr;
  FreeNode* prev = nullptr;
  FreeNode* pos = nullptr;
  for (size_t i = 0; i < kFreeListGroupSize; i++) {
    prev = &dummys_[i];
    pos = dummys_[i].next;
    while (pos != nullptr) {
      if (pos->start_index + pos->block_count == mutation.start_index) {
        // we find the prev free node
        prev_free = pos;
        prev->next = pos->next;
        pos->next = nullptr;
        pos = prev->next;
        continue;
      }
      if (pos->start_index == mutation.start_index + mutation.block_count) {
        // we find the next free node
        next_free = pos;
        prev->next = pos->next;
        pos->next = nullptr;
        pos = prev->next;
        continue;
      }
      prev = pos;
      pos = pos->next;
    }
  }

  FreeNode* new_free_node =
      new FreeNode(mutation.start_index, mutation.block_count);
  if (prev_free != nullptr) {
    new_free_node->start_index = prev_free->start_index;
    new_free_node->block_count += prev_free->block_count;
    delete prev_free;
  }
  if (next_free != nullptr) {
    new_free_node->block_count += next_free->block_count;
    delete next_free;
  }

  auto dummy = &dummys_[CalcFreeListIndex(new_free_node->block_count)];
  new_free_node->next = dummy->next;
  dummy->next = new_free_node;
}

auto HeapFile::CalcFreeListIndex(uint32_t block_count) -> size_t {
  for (size_t i = 0; i < kFreeListGroupSize; i++) {
    if ((1 << i) >= block_count) {
      return i;
    }
  }
  return kFreeListGroupSize - 1;
}

}  // namespace leveldb