#ifndef STORAGE_LEVELDB_HEAPFILE_HEAPFILE_H_
#define STORAGE_LEVELDB_HEAPFILE_HEAPFILE_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "leveldb/options.h"
#include "leveldb/status.h"

#include "util/coding.h"
#include "util/crc32c.h"
#include "util/io_uring.h"

namespace leveldb {

// 4k block
static const uint64_t kHeapBlockSize = 4096;
// 4k block - uint32_t crc32c
static const uint64_t kBitmapSize = kHeapBlockSize - sizeof(uint32_t);
// super block and 4092 bytes for bitmap
static const uint64_t kHeapFileSize = kHeapBlockSize * (1 + 8 * kBitmapSize);
// super block is at the end of the file
static const uint64_t kSuperBlockOffset = kHeapBlockSize * 8 * kBitmapSize;
// for 4092 bytes bitmap, uint32_t is enough (we might increase file size in the
// future so 16bit is not enough)
static const uint32_t kInvalidBitMapIndex =
    std::numeric_limits<uint32_t>::max();

static const uint64_t kInvalidOffset = std::numeric_limits<uint64_t>::max();

template <const size_t N = kHeapBlockSize>
static inline auto round_up(uint64_t nbytes) -> uint64_t {
  return (nbytes + N - 1) & ~(N - 1);
}

template <const size_t N = kHeapBlockSize>
static inline auto round_down(uint64_t nbytes) -> uint64_t {
  return nbytes & ~(N - 1);
}

class HeapFile;

struct SuperBlock {
  uint32_t crc;
  uint8_t bitmap[kBitmapSize];
  auto CalcCheckSum() const -> uint32_t {
    return crc32c::Value(reinterpret_cast<const char*>(bitmap), kBitmapSize);
  }
  auto VerifyCheckSum() const -> bool {
    return crc32c::Unmask(DecodeFixed32(reinterpret_cast<const char*>(&crc))) ==
           CalcCheckSum();
  }
  auto SetCheckSum() -> void {
    EncodeFixed32(reinterpret_cast<char*>(&crc), crc32c::Mask(CalcCheckSum()));
  }
  auto At(uint32_t index) const -> uint8_t {
    return !!(bitmap[index >> 3] & (1 << (7 - index & 0x7)));
  }
  auto Set(uint32_t index) -> void {
    bitmap[index >> 3] |= (1 << (7 - index & 0x7));
  }
  auto UnSet(uint32_t index) -> void {
    bitmap[index >> 3] &= ~(1 << (7 - index & 0x7));
  }
};

class HeapFile {
 public:
  class Mutation {
    friend class HeapFile;

   private:
    uint32_t start_index;
    uint32_t block_count;

   public:
    Mutation(uint32_t start_index = kInvalidBitMapIndex,
             uint32_t block_count = 0)
        : start_index(start_index), block_count(block_count) {}
    auto StartIndex() const -> uint32_t { return start_index; }
    auto BlockCount() const -> uint32_t { return block_count; }
    auto Offset() const -> uint64_t {
      return start_index == kInvalidBitMapIndex ? kInvalidOffset
                                                : start_index * kHeapBlockSize;
    }
    auto Size() const -> uint64_t {
      return start_index == kInvalidBitMapIndex ? 0
                                                : block_count * kHeapBlockSize;
    }
  };

 private:
  struct FreeNode {
    FreeNode* next;
    // free block index in bitmap
    uint32_t start_index;
    // how much free blocks start from start_index
    uint32_t block_count;
    FreeNode(uint32_t start_index = kInvalidBitMapIndex,
             uint32_t block_count = 0)
        : next(nullptr), start_index(start_index), block_count(block_count) {}
    auto Size() const -> uint64_t { return block_count * kHeapBlockSize; }
  };

 private:
  std::mutex mu_;
  uint64_t file_number_;
  int fd_;
  uint32_t free_block_count_;
  uint64_t cache_id_;
  // we don't alloc it as array here as it should be posix_memalign
  SuperBlock* super_block_;

  // [4K, 8K, 16K, 32K, 64K, 128K, 256K, 512K, 1M, >=1M]
  static const size_t kFreeListGroupSize = 10;
  // free block list.
  FreeNode dummys_[kFreeListGroupSize];

 public:
  static auto Open(const Options* options, const std::string& dbname,
                   uint64_t file_number, std::unique_ptr<HeapFile>* heap_file)
      -> Status;

 public:
  HeapFile(HeapFile&&) = delete;
  HeapFile(const HeapFile&) = delete;
  ~HeapFile();
  auto FileNumber() const -> uint64_t { return file_number_; }
  auto Fd() const -> int { return fd_; }
  auto CacheId() const -> uint64_t { return cache_id_; }
  auto FreeBlockCount() const -> uint32_t { return free_block_count_; }
  // auto SuperBlock() const -> const SuperBlock* { return super_block_; }

  // will modify: **free list**
  //
  // this is for flush job to allocate blocks for values.
  // return the offset and size of the allocated blocks.
  // offset will be set to kInvalidOffset if no block is allocated.
  auto AllocBlocks(uint64_t nbytes) -> Mutation;
  // will modify: **free list**
  //
  // this is for failed flush job to release allocated blocks.
  auto ReleaseBlocks(const std::vector<Mutation>& mutations) -> void;
  // will modify: **super block**
  //
  // this is for flush job to submit changes
  auto SetBitmapAndFlush(const std::vector<Mutation>& mutations) -> Status;
  // will modify: **free list**, **super block**
  //
  // for compaction to remove values.
  auto UnSetBitmapAndFlush(const std::vector<Mutation>& mutations) -> Status;

 private:
  HeapFile(uint64_t file_number, int fd, uint64_t cache_id,
           SuperBlock* super_block);
  auto InitFreeBlockList() -> void;
  // release related blocks to free list.
  auto ReleaseBlock(const Mutation& mutation) -> void;
  static auto CalcFreeListIndex(uint32_t block_count) -> size_t;
};
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_HEAPFILE_HEAPFILE_H_