#include "heapfile/heapfile_value.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include "leveldb/options.h"
#include "leveldb/status.h"

#include "port/port_stdcxx.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include "heapfile/heapfile.h"

namespace leveldb {

auto HFValueMeta::DecodeFrom(Slice& s) -> HFValueMeta {
  HFValueMeta meta;
  GetVarint64(&s, &meta.file_number);
  GetVarint32(&s, &meta.start_index);
  GetVarint32(&s, &meta.block_count);
  return meta;
}

auto HFValueMeta::EncodeTo(std::string& s) const -> void {
  PutVarint64(&s, file_number);
  PutVarint32(&s, start_index);
  PutVarint32(&s, block_count);
}

auto HFValueMaxEncodedLength(const Options& options, size_t source_bytes)
    -> size_t {
  size_t raw = kHFValueHeaderSize + source_bytes;
  switch (options.compression) {
    case kNoCompression:
      return raw;
    case kSnappyCompression:
      if (!port::Have_Snappy()) {
        return raw;
      }
      size_t mcl;
      port::Snappy_MaxCompressedLength(source_bytes, &mcl);
      return std::max(raw, kHFValueHeaderSize + mcl);
  }
  return raw;
}

auto EncodeHFValue(const Options& options, const Slice& value, uint8_t* buf,
                   size_t* output_size) -> Status {
  CompressionType t = options.compression;
  // TODO: support other compression type
  if (!port::Have_Snappy() && t == kSnappyCompression) {
    t = kNoCompression;
  }
  size_t raw_size = kHFValueHeaderSize + value.size();
  if (raw_size <= kHeapBlockSize) {
    t = kNoCompression;
  }

  size_t size;
  switch (t) {
    case kNoCompression: {
      size = raw_size;
      buf[4] = t;
      EncodeFixed64(reinterpret_cast<char*>(buf + 5), value.size());
      memcpy(buf + kHFValueHeaderSize, value.data(), value.size());
      break;
    }
    case kSnappyCompression: {
      port::Snappy_Compress(value.data(), value.size(),
                            reinterpret_cast<char*>(buf + kHFValueHeaderSize),
                            &size);
      if (round_up(size + kHFValueHeaderSize) < round_up(raw_size)) {
        buf[4] = t;
        EncodeFixed64(reinterpret_cast<char*>(buf + 5), size);
        size += kHFValueHeaderSize;
      } else {
        size = kHFValueHeaderSize + value.size();
        buf[4] = kNoCompression;
        EncodeFixed64(reinterpret_cast<char*>(buf + 5), value.size());
        memcpy(buf + kHFValueHeaderSize, value.data(), value.size());
      }
      break;
    }
  }
  *output_size = size;
  uint32_t crc = crc32c::Value(reinterpret_cast<char*>(buf + 4), size - 4);
  EncodeFixed32(reinterpret_cast<char*>(buf), crc32c::Mask(crc));
  return Status::OK();
}

auto DecodeHFValue(const uint8_t* value, size_t length, uint8_t** output)
    -> Status {
  uint64_t len = DecodeFixed64(reinterpret_cast<const char*>(value + 5));

  uint32_t expected_crc =
      crc32c::Unmask(DecodeFixed32(reinterpret_cast<const char*>(value)));
  uint32_t actual_crc = crc32c::Value(reinterpret_cast<const char*>(value + 4),
                                      kHFValueHeaderSize - 4 + len);
  if (actual_crc != expected_crc) {
    return Status::Corruption("corrupted");
  }

  CompressionType t = static_cast<CompressionType>(value[4]);
  size_t size;
  switch (t) {
    case kNoCompression: {
      *output = new uint8_t[len];
      memcpy(*output, value + kHFValueHeaderSize, len);
      break;
    }
    case kSnappyCompression: {
      if (!port::Have_Snappy()) {
        return Status::Corruption("corrupted value");
      }
      size_t uncompressed_length;
      if (!port::Snappy_GetUncompressedLength(
              reinterpret_cast<const char*>(value + kHFValueHeaderSize), len,
              &uncompressed_length)) {
        return Status::Corruption("corrupted value");
      }
      *output = new uint8_t[uncompressed_length];
      if (!port::Snappy_Uncompress(
              reinterpret_cast<const char*>(value + kHFValueHeaderSize), len,
              reinterpret_cast<char*>(*output))) {
        return Status::Corruption("corrupted value");
      }
      break;
    }
  }
  return Status::OK();
}

}  // namespace leveldb