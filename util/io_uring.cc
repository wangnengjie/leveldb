#include "io_uring.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <memory>
#include <mutex>
#include <vector>

#include "leveldb/status.h"

namespace leveldb {

auto Handle::Init(IoUring* ring, const IoReq& req) -> void {
  done_ = false;
  write_ = req.write;
  fd_ = req.fd;
  status_ = Status::OK();
  ring_ = ring;
  nbytes_ = req.nbytes;
  offset_ = req.offset;
  buf_ = req.buf;
  ptr_ = buf_;
}

auto Handle::Update(uint64_t bytes_done) -> void {
  assert(nbytes_ >= bytes_done);
  nbytes_ -= bytes_done;
  ptr_ += bytes_done;
  offset_ += bytes_done;
  // TODO: check direct io ?
  if (bytes_done == 0 || nbytes_ == 0) {
    done_ = true;
  }
}

auto Handle::PrepSqe(io_uring_sqe* sqe) -> void {
  if (write_) {
    io_uring_prep_write(sqe, fd_, ptr_, nbytes_, offset_);
  } else {
    io_uring_prep_read(sqe, fd_, ptr_, nbytes_, offset_);
  }
  io_uring_sqe_set_data(sqe, this);
}

auto Handle::Wait() -> void {
  if (done_) {
    return;
  }
  IoUring& inst = IoUring::Instance();
  assert(ring_ == &inst);
  while (!done_) {
    inst.PollAndHandleCQE();
  }
}

auto IoUring::Instance() -> IoUring& {
  static std::mutex mu;
  static int wq_fd = 0;
  static thread_local std::unique_ptr<IoUring> uring = nullptr;
  if (uring == nullptr) {
    std::lock_guard<std::mutex> lk(mu);
    uring = std::unique_ptr<IoUring>(new IoUring(kIoUringDepth, wq_fd));
    if (wq_fd == 0) {
      wq_fd = uring->RingFd();
    }
  }
  return *uring.get();
}

IoUring::IoUring(uint16_t depth, int wq_fd, bool io_poll, bool sq_poll)
    : depth_(depth), in_flight_(0) {
  struct io_uring_params p;
  memset(&p, 0, sizeof(p));
  p.wq_fd = wq_fd;
  if (wq_fd != 0) {
    p.flags |= IORING_SETUP_ATTACH_WQ;
  }
  if (io_poll) {
    p.flags |= IORING_SETUP_IOPOLL;
  }
  if (sq_poll) {
    p.flags |= IORING_SETUP_SQPOLL;
  }
  int ret = io_uring_queue_init_params(depth, &io_uring_, &p);
  assert(ret == 0);
}

IoUring::~IoUring() {
  while (in_flight_ > 0) {
    PollAndHandleCQE();
  }
  io_uring_queue_exit(&io_uring_);
}

auto IoUring::DoIoReq(const IoReq& req) -> HandleWrapper {
  while (in_flight_ >= depth_) {
    PollAndHandleCQE();
  }

  Handle* h = new Handle();
  h->Init(this, req);
  io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
  assert(sqe != nullptr);  // we limit in_flight_ to depth_
  h->PrepSqe(sqe);
  auto num = io_uring_submit(&io_uring_);
  in_flight_++;
  return HandleWrapper(h);
}

auto IoUring::DoIoReqSync(const IoReq& req) -> Status {
  HandleWrapper hw = DoIoReq(req);
  hw->Wait();
  return hw->status();
}

auto IoUring::BatchIoReq(const std::vector<IoReq>& reqs)
    -> std::vector<HandleWrapper> {
  if (reqs.empty()) {
    return {};
  }

  // TODO: auto split reqs into multiple batches?
  assert(reqs.size() <= depth_);

  while (in_flight_ + reqs.size() > depth_) {
    PollAndHandleCQE();
  }
  std::vector<HandleWrapper> hws;
  hws.reserve(reqs.size());
  for (const auto& req : reqs) {
    Handle* h = new Handle();
    h->Init(this, req);
    io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
    assert(sqe != nullptr);  // we limit in_flight_ to depth_
    h->PrepSqe(sqe);
    hws.emplace_back(h);
  }
  io_uring_submit(&io_uring_);
  in_flight_ += reqs.size();
  return hws;
}

auto IoUring::BatchIoReqSync(const std::vector<IoReq>& reqs)
    -> std::vector<Status> {
  std::vector<HandleWrapper> hws = BatchIoReq(reqs);
  for (auto& hw : hws) {
    hw->Wait();
  }
  std::vector<Status> statuses;
  statuses.reserve(reqs.size());
  for (auto& hw : hws) {
    statuses.push_back(std::move(hw->status()));
  }
  return statuses;
}

auto IoUring::PollAndHandleCQE() -> void {
  io_uring_cqe* cqes[kIoUringDepth];
  size_t num = io_uring_peek_batch_cqe(&io_uring_, cqes, kIoUringDepth);
  if (num == 0) {
    int ret = io_uring_wait_cqe(&io_uring_, &cqes[0]);
    assert(ret == 0);
    num = 1;
  }

  int to_resubmit = 0;

  for (size_t i = 0; i < num; i++) {
    io_uring_cqe* cqe = cqes[i];
    if (HandleCQE(cqe)) {
      to_resubmit++;
    }
  }

  if (to_resubmit > 0) {
    io_uring_submit(&io_uring_);
  }
}

auto IoUring::TryPollAndHandleCQE() -> bool {
  io_uring_cqe* cqes[kIoUringDepth];
  size_t num = io_uring_peek_batch_cqe(&io_uring_, cqes, kIoUringDepth);
  if (num == 0) {
    return false;
  }

  int to_resubmit = 0;

  for (size_t i = 0; i < num; i++) {
    io_uring_cqe* cqe = cqes[i];
    if (HandleCQE(cqe)) {
      to_resubmit++;
    }
  }
  if (to_resubmit > 0) {
    io_uring_submit(&io_uring_);
  }
  return true;
}

auto IoUring::HandleCQE(io_uring_cqe* cqe) -> bool {
  Handle* handle = reinterpret_cast<Handle*>(io_uring_cqe_get_data(cqe));
  if (cqe->res < 0) {
    handle->status_ = Status::IOError("io_uring error", strerror(errno));
    handle->done_ = true;
    in_flight_--;
    return false;
  }
  // assert(cqe->res >= 0);  // TODO: handle error
  handle->Update(cqe->res);
  if (handle->Done()) {
    // don't release handle here, it will be released when HandleWrapper drop
    in_flight_--;
  } else {
    io_uring_sqe* sqe = io_uring_get_sqe(&io_uring_);
    // we limit in_flight_ to depth_, and there is an entry completed
    assert(sqe != nullptr);
    handle->PrepSqe(sqe);
  }
  io_uring_cqe_seen(&io_uring_, cqe);
  return !handle->Done();
}

}  // namespace leveldb