#ifndef STORAGE_LEVELDB_UTIL_IO_URING_H_
#define STORAGE_LEVELDB_UTIL_IO_URING_H_

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <liburing.h>
#include <memory>
#include <mutex>
#include <vector>

#include "leveldb/status.h"

namespace leveldb {

static const size_t kHandlePoolSize = 1024;
static const size_t kIoUringDepth = 128;

struct IoReq {
  bool write;
  int fd;
  uint64_t nbytes;
  uint64_t offset;
  uint8_t* buf;
  IoReq(bool write, int fd, uint64_t nbytes, uint64_t offset, uint8_t* buf)
      : write(write), fd(fd), nbytes(nbytes), offset(offset), buf(buf) {}
};

class IoUring;

class Handle {
  friend class HandlePool;
  friend class IoUring;

 private:
  bool done_;
  bool write_;
  int fd_;
  Status status_;
  IoUring* ring_;
  uint64_t nbytes_;
  uint64_t offset_;
  uint8_t* buf_;
  uint8_t* ptr_;

 private:
  auto Init(IoUring* ring, const IoReq& req) -> void;
  auto Update(uint64_t bytes_done) -> void;
  auto PrepSqe(io_uring_sqe* sqe) -> void;

 public:
  Handle()
      : done_(false),
        write_(false),
        fd_(0),
        ring_(nullptr),
        nbytes_(0),
        offset_(0),
        buf_(nullptr),
        ptr_(nullptr){};
  auto Wait() -> void;
  auto Buffer() -> uint8_t* { return buf_; };
  auto Buffer() const -> const uint8_t* { return buf_; };
  auto IsWrite() const -> bool { return write_; }
  auto Done() const -> bool { return done_; }
  auto Size() const -> uint64_t { return ptr_ - buf_; };
  auto status() const -> Status { return status_; }
};

class HandleWrapper {
 private:
  Handle* handle_ = nullptr;

 public:
  explicit HandleWrapper(Handle* handle = nullptr) : handle_(handle) {}
  HandleWrapper(const HandleWrapper&) = delete;
  HandleWrapper& operator=(const HandleWrapper&) = delete;
  HandleWrapper(HandleWrapper&& hw) {
    handle_ = hw.handle_;
    hw.handle_ = nullptr;
  }
  HandleWrapper& operator=(HandleWrapper&& hw) {
    if (handle_ != nullptr) {
      handle_->Wait();
      delete handle_;
    }
    handle_ = hw.handle_;
    hw.handle_ = nullptr;
    return *this;
  }
  ~HandleWrapper() {
    if (handle_ != nullptr) {
      handle_->Wait();
      delete handle_;
    }
  }
  auto operator*() -> Handle* { return handle_; }
  auto operator->() -> Handle* { return handle_; }
};

// for direct io only.
class IoUring {
 public:
  static auto Instance() -> IoUring&;

 private:
  uint16_t depth_;
  uint16_t in_flight_;
  io_uring io_uring_;

 public:
  IoUring(const IoUring&) = delete;
  IoUring(IoUring&&) = delete;
  ~IoUring();
  auto DoIoReq(const IoReq& req) -> HandleWrapper;
  auto DoIoReqSync(const IoReq& req) -> Status;
  auto BatchIoReq(const std::vector<IoReq>& reqs) -> std::vector<HandleWrapper>;
  auto BatchIoReqSync(const std::vector<IoReq>& reqs) -> std::vector<Status>;

  auto PollAndHandleCQE() -> void;
  auto TryPollAndHandleCQE() -> bool;

 private:
  IoUring(uint16_t depth, int wq_fd, bool io_poll = false,
          bool sq_poll = false);
  auto RingFd() -> int { return io_uring_.ring_fd; }
  // return true if entry need resubmit.
  auto HandleCQE(io_uring_cqe* cqe) -> bool;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_IO_URING_H_