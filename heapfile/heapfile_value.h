#ifndef STORAGE_LEVELDB_HEAPFILE_VALUE_H_
#define STORAGE_LEVELDB_HEAPFILE_VALUE_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

#include "util/coding.h"

namespace leveldb {

static const size_t kHFValueHeaderSize = 4 + 1 + 8;

struct HFValueMeta {
  uint64_t file_number;
  uint32_t start_index;
  uint32_t block_count;
  auto DecodeFrom(Slice& s) -> HFValueMeta;
  auto EncodeTo(std::string& s) const -> void;
};

// value format:
// | crc32 (4B) | type (1B) | uint64(8B) | encoded value |

static auto MaxEncodedLength(const Options& options, size_t source_bytes)
    -> size_t;

static auto EncodeHFValue(const Options& options, const Slice& value,
                          uint8_t* buf, size_t* output_size) -> Status;

static auto DecodeHFValue(const uint8_t* value, size_t length, uint8_t** output)
    -> Status;
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_HEAPFILE_VALUE_H_