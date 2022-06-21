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
  bool done_ = false;
  bool fixed_ = false;
  bool write_ = false;
  int fd_ = 0;
  Status status_;
  Handle* next_ = nullptr;
  IoUring* ring_ = nullptr;
  uint64_t nbytes_ = 0;
  uint64_t offset_ = 0;
  uint8_t* buf_ = nullptr;
  uint8_t* ptr_ = nullptr;

 private:
  auto Init(IoUring* ring, const IoReq& req) -> void;
  auto Update(uint64_t bytes_done) -> void;
  auto PrepSqe(io_uring_sqe* sqe) -> void;

 public:
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
  HandleWrapper(Handle* handle) : handle_(handle) {}
  HandleWrapper(HandleWrapper&& hw);
  ~HandleWrapper();

  HandleWrapper(const HandleWrapper&) = delete;
  HandleWrapper& operator=(const HandleWrapper&) = delete;
  HandleWrapper& operator=(HandleWrapper&&);

  auto Get() -> Handle& { return *handle_; }
  auto Done() const -> bool { return handle_ == nullptr || handle_->Done(); }
};

class HandlePool {
  friend Handle;
  friend HandleWrapper;

 private:
  std::mutex mu_;
  Handle dummy_;
  Handle pool_[kHandlePoolSize];
  HandlePool();
  static auto Instance() -> HandlePool&;
  static auto Release(Handle* handle) -> void;

 public:
  // get a handle from the pool.
  // the handle will be released when the wrapper is dropped.
  static auto Acquire() -> HandleWrapper;
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
  auto DoIoReqSync(const IoReq& req) -> void { DoIoReq(req); }
  auto BatchIoReq(const std::vector<IoReq>& reqs) -> std::vector<HandleWrapper>;
  auto BatchIoReqSync(const std::vector<IoReq>& reqs) -> void {
    BatchIoReq(reqs);
  }

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