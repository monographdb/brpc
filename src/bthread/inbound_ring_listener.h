#pragma once

#include <condition_variable>
#include <glog/logging.h>
#include <iostream>
#include <liburing.h>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "brpc/socket.h"
#include "bthread/inbound_ring_buf.h"
#include "bthread/task_group.h"
#include "butil/threading/platform_thread.h"

#ifndef IORING_RECVSEND_BUNDLE
#define IORING_RECVSEND_BUNDLE (1U << 4)
#endif

class InboundRingListener {
public:
  InboundRingListener(bthread::TaskGroup *group) : task_group_(group) {}

  ~InboundRingListener() {
    for (auto [fd, fd_idx] : reg_fds_) {
      SubmitCancel(fd);
    }
    SubmitAll();

    poll_status_.store(PollStatus::Closed, std::memory_order_release);
    {
      std::unique_lock<std::mutex> lk(mux_);
      cv_.notify_one();
    }

    if (poll_thd_.joinable()) {
      poll_thd_.join();
    }
    Close();
  }

  int Init() {
    int ret = io_uring_queue_init(1024, &ring_, IORING_SETUP_SINGLE_ISSUER);

    if (ret < 0) {
      LOG(WARNING) << "Failed to initialize the IO uring of the inbound "
                      "listener, errno: "
                   << ret;
      Close();
      return ret;
    }
    ring_init_ = true;

    ret = io_uring_register_files_sparse(&ring_, 1024);
    if (ret < 0) {
      LOG(WARNING)
          << "Failed to register sparse files for the inbound listener.";
      Close();
      return ret;
    }

    free_reg_fd_idx_.reserve(1024);
    for (uint16_t f_idx = 0; f_idx < 1024; ++f_idx) {
      free_reg_fd_idx_.emplace_back(f_idx);
    }

    buf_ = (char *)std::aligned_alloc(buf_length, buf_length * buf_ring_size);
    buf_ring_ = io_uring_setup_buf_ring(&ring_, buf_ring_size, 0, 0, &ret);
    if (buf_ring_ == nullptr) {
      LOG(WARNING)
          << "Failed to register buffer ring for the inbound listener.";
      Close();
      return -1;
    }

    char *ptr = buf_;
    // inbound_ring_size must be the power of 2.
    int br_mask = buf_ring_size - 1;
    for (size_t idx = 0; idx < buf_ring_size; idx++) {
      io_uring_buf_ring_add(buf_ring_, ptr, buf_length, idx, br_mask, idx);
      ptr += buf_length;
    }
    io_uring_buf_ring_advance(buf_ring_, buf_ring_size);

    poll_status_.store(PollStatus::Active, std::memory_order_release);
    poll_thd_ = std::thread([&]() {
      std::string ring_listener = "ring_listener:";
      ring_listener.append(std::to_string(task_group_->group_id_));
      butil::PlatformThread::SetName(ring_listener.c_str());

      Run();
    });

    return 0;
  }

  void Close() {
    if (ring_init_) {
      io_uring_queue_exit(&ring_);
      ring_init_ = false;
    }

    if (buf_) {
      free(buf_);
      buf_ = nullptr;
    }
  }

  int Register(brpc::Socket *sock) {
    int fd = sock->fd();
    assert(fd >= 0);

    auto it = reg_fds_.find(fd);
    if (it != reg_fds_.end()) {
      LOG(WARNING) << "Socket " << sock->id() << ", fd: " << sock->fd()
                   << " has been registered before.";
      return -1;
    }

    sock->reg_fd_idx_ = -1;
    int ret = -1;

    if (free_reg_fd_idx_.empty()) {
      // All registered file slots have been taken. Cannot register the socket's
      // fd.
      reg_fds_.try_emplace(fd, -1);
      ret = SubmitRecv(sock);
    } else {
      uint16_t fd_idx = free_reg_fd_idx_.back();
      free_reg_fd_idx_.pop_back();
      reg_fds_.try_emplace(fd, fd_idx);
      sock->reg_fd_ = fd;
      ret = SubmitRegisterFile(sock, &sock->reg_fd_, fd_idx);
    }

    if (ret < 0) {
      reg_fds_.erase(fd);
      return -1;
    }

    reg_cnt_.fetch_add(1, std::memory_order_release);
    // uint32_t prev_reg_cnt = reg_cnt_.fetch_add(1, std::memory_order_release);
    // if (prev_reg_cnt == 0 &&
    //     poll_status_.load(std::memory_order_relaxed) == PollStatus::Sleep) {
    //   std::unique_lock<std::mutex> lk(mux_);
    //   cv_.notify_one();
    // }

    return 0;
  }

  int SubmitRecv(brpc::Socket *sock) {
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      LOG(ERROR) << "IO uring submission queue is full for the inbound "
                    "listener, group: "
                 << task_group_->group_id_;
      return -1;
    }
    int fd_idx = sock->reg_fd_idx_;
    int sfd = fd_idx >= 0 ? fd_idx : sock->fd();
    io_uring_prep_recv_multishot(sqe, sfd, NULL, 0, 0);
    uint64_t data = reinterpret_cast<uint64_t>(sock);
    data = data << 16;
    data |= (uint8_t)OpCode::Recv;
    io_uring_sqe_set_data64(sqe, data);

    sqe->buf_group = 0;
    sqe->flags |= IOSQE_BUFFER_SELECT;
    if (fd_idx >= 0) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    // sqe->ioprio |= IORING_RECVSEND_BUNDLE;

    ++submit_cnt_;
    return 0;
  }

  int SubmitAll() {
    if (submit_cnt_ == 0) {
      return 0;
    }

    int ret = io_uring_submit(&ring_);
    if (ret >= 0) {
      submit_cnt_ = submit_cnt_ >= ret ? submit_cnt_ - ret : 0;
    } else {
      // IO uring submission failed. Clears the submission count.
      submit_cnt_ = 0;
      LOG(ERROR) << "Failed to flush the IO uring submission queue for the "
                    "inbound listener.";
    }
    return ret;
  }

  void Unregister(int fd) { SubmitCancel(fd); }

  void PollRecv() {
    io_uring_cqe *cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) {
      LOG(ERROR) << "Listener uring wait errno: " << ret;
      return;
    }

    int processed = 0;
    unsigned int head;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      HandleCqe(cqe);
      ++processed;
    }

    if (processed > 0) {
      io_uring_cq_advance(&ring_, processed);
    }
  }

  void ExtPollRecv() {
    if (!has_external_.load(std::memory_order_relaxed)) {
      has_external_.store(true, std::memory_order_release);
    }

    // has_external_ should be updated before poll_status_ is checked.
    std::atomic_thread_fence(std::memory_order_release);

    if (poll_status_.load(std::memory_order_acquire) != PollStatus::Sleep) {
      return;
    }

    io_uring_cqe *cqe = nullptr;
    int ret = io_uring_peek_cqe(&ring_, &cqe);
    if (ret != 0) {
      return;
    }

    int processed = 0;
    unsigned int head;
    io_uring_for_each_cqe(&ring_, head, cqe) {
      HandleCqe(cqe);
      ++processed;
    }

    if (processed > 0) {
      io_uring_cq_advance(&ring_, processed);
    }
  }

  void ExtWakeup() {
    has_external_.store(false, std::memory_order_relaxed);
    std::unique_lock<std::mutex> lk(mux_);
    cv_.notify_one();
  }

  void Run() {
    while (poll_status_.load(std::memory_order_relaxed) != PollStatus::Closed) {
      if (reg_cnt_.load(std::memory_order_acquire) > 0 &&
          !has_external_.load(std::memory_order_relaxed)) {
        PollRecv();
      } else {
        PollStatus sta = PollStatus::Active;
        if (poll_status_.compare_exchange_strong(sta, PollStatus::Sleep,
                                                 std::memory_order_acq_rel)) {
          std::unique_lock<std::mutex> lk(mux_);
          cv_.wait(lk, [this]() {
            return (reg_cnt_.load(std::memory_order_acquire) > 0 &&
                    !has_external_.load(std::memory_order_relaxed)) ||
                   poll_status_.load(std::memory_order_relaxed) ==
                       PollStatus::Closed;
          });

          sta = PollStatus::Sleep;
          // If the poll thread is woken up due to new inbound messages, sets
          // the polling status to active. Or, the polling status must be
          // closed.
          poll_status_.compare_exchange_strong(sta, PollStatus::Active,
                                               std::memory_order_acq_rel);
        }
      }
    }
  }

  void ReturnInboundBuf(uint16_t bid, size_t bytes) {
    // The socket has finished processing inbound messages. Returns the borrowed
    // buffers to the buffer ring.
    int br_mask = buf_ring_size - 1;
    int buf_cnt = 0;
    while (bytes > 0) {
      char *this_buf = buf_ + bid * buf_length;
      io_uring_buf_ring_add(buf_ring_, this_buf, buf_length, bid, br_mask,
                            buf_cnt);

      bytes = bytes > buf_length ? bytes - buf_length : 0;
      bid = (bid + 1) & br_mask;
      buf_cnt++;
    }
    io_uring_buf_ring_advance(buf_ring_, buf_cnt);

    // If there are completion events with the ENOBUFS error, re-submits
    // requests when ring buffers are returned.
    if (waiting_cnt_.load(std::memory_order_relaxed) > 0) {
      size_t cnt =
          waiting_socks_.TryDequeueBulk(waiting_batch_.begin(), buf_cnt);
      for (size_t idx = 0; idx < cnt; ++idx) {
        SubmitRecv(waiting_batch_[idx]);
      }
      waiting_cnt_.fetch_sub(cnt, std::memory_order_release);
    }
  }

  const char *GetRingBuf(uint16_t bid) const { return buf_ + bid * buf_length; }

private:
  void FreeBuf() {
    if (buf_ != nullptr) {
      free(buf_);
    }
  }

  int SubmitCancel(int fd) {
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      LOG(ERROR) << "IO uring submission queue is full for the inbound "
                    "listener, group: "
                 << task_group_->group_id_;
      return -1;
    }

    auto it = reg_fds_.find(fd);
    if (it == reg_fds_.end()) {
      return 0;
    }

    int fd_idx = it->second;
    int sfd = fd;
    uint64_t data = 0;

    int flags = 0;
    if (fd_idx >= 0) {
      flags |= IORING_ASYNC_CANCEL_FD_FIXED;
      sfd = fd_idx;
      data = fd_idx << 16;
    }

    io_uring_prep_cancel_fd(sqe, sfd, flags);
    data |= (uint8_t)OpCode::CancelRecv;
    io_uring_sqe_set_data64(sqe, data);
    if (fd_idx >= 0) {
      sqe->cancel_flags |= IOSQE_FIXED_FILE;
    }

    reg_fds_.erase(it);
    submit_cnt_++;
    return 0;
  }

  int SubmitRegisterFile(brpc::Socket *sock, int *fd, int32_t fd_idx) {
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      LOG(ERROR) << "IO uring submission queue is full for the inbound "
                    "listener, group: "
                 << task_group_->group_id_;
      return -1;
    }

    io_uring_prep_files_update(sqe, fd, 1, fd_idx);
    uint64_t data = reinterpret_cast<uint64_t>(sock);
    data = data << 16;
    data |= (uint8_t)OpCode::RegisterFile;
    io_uring_sqe_set_data64(sqe, data);
    sock->reg_fd_idx_ = fd_idx;

    ++submit_cnt_;
    return 0;
  }

  void HandleCqe(io_uring_cqe *cqe) {
    uint64_t data = io_uring_cqe_get_data64(cqe);
    OpCode op = (OpCode)(data & UINT16_MAX);
    data = data >> 16;

    switch (op) {
    case OpCode::Recv: {
      brpc::Socket *sock = reinterpret_cast<brpc::Socket *>(data);
      HandleRecv(cqe, sock);
      break;
    }
    case OpCode::CancelRecv: {
      if (cqe->res < 0) {
        LOG(ERROR) << "Failed to cancel socket recv, errno: " << cqe->res
                   << ", group: " << task_group_->group_id_;
      }
      uint16_t fd_idx = (uint16_t)data;
      free_reg_fd_idx_.emplace_back(fd_idx);
      break;
    }
    case OpCode::RegisterFile: {
      brpc::Socket *sock = reinterpret_cast<brpc::Socket *>(data);
      if (cqe->res < 0) {
        LOG(WARNING) << "IO uring file registration failed, errno: " << cqe->res
                     << ", group: " << task_group_->group_id_
                     << ", socket: " << sock->id() << ", fd: " << sock->fd();
        free_reg_fd_idx_.emplace_back(sock->reg_fd_idx_);
        sock->reg_fd_idx_ = -1;
        auto it = reg_fds_.find(sock->fd());
        assert(it != reg_fds_.end());
        it->second = -1;
      }
      SubmitRecv(sock);
      break;
    }
    default:
      break;
    }
  }

  enum struct OpCode : uint8_t { Recv = 0, CancelRecv, RegisterFile };

  void HandleRecv(io_uring_cqe *cqe, brpc::Socket *sock) {
    int32_t nw = cqe->res;
    uint16_t buf_id = UINT16_MAX;
    bool need_rearm = false;

    assert(sock != nullptr);

    if (nw < 0) {
      int err = -nw;
      if (err == ENOBUFS) {
        waiting_cnt_.fetch_add(1, std::memory_order_relaxed);
        if (waiting_socks_.TryEnqueue(sock)) {
          return;
        } else {
          waiting_cnt_.fetch_sub(1, std::memory_order_relaxed);
        }
      }

      if (err == EAGAIN || err == EINTR || err == ENOBUFS) {
        need_rearm = true;
      }
    } else {
      // Not having a buffer attached should only happen if we get a zero sized
      // receive, because the other end closed the connection. It cannot happen
      // otherwise, as all our receives are using provided buffers and hence
      // it's not possible to return a CQE with a non-zero result and not have a
      // buffer attached.
      if (cqe->flags & IORING_CQE_F_BUFFER) {
        buf_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
        assert(nw > 0);
      }

      // If IORING_CQE_F_MORE isn't set, this multishot recv won't post any
      // further completions.
      if (!(cqe->flags & IORING_CQE_F_MORE)) {
        need_rearm = true;
      }
    }

    bool success =
        task_group_->EnqueueInboundRingBuf(sock, nw, buf_id, need_rearm);
    LOG_IF(FATAL, !success) << "Inbound ring buffer failed to enqueue, group: "
                            << task_group_->group_id_;
  }

  enum struct PollStatus : uint8_t { Active = 0, Sleep, Closed };

  struct io_uring ring_;
  bool ring_init_{false};
  std::atomic<PollStatus> poll_status_{PollStatus::Active};
  uint16_t submit_cnt_{0};
  std::atomic<uint32_t> reg_cnt_{0};
  std::unordered_map<int, int> reg_fds_;
  std::mutex mux_;
  std::condition_variable cv_;
  std::thread poll_thd_;

  io_uring_buf_ring *buf_ring_{nullptr};
  char *buf_{nullptr};

  bthread::TaskGroup *task_group_{nullptr};

  eloq::SpscQueue<brpc::Socket *> waiting_socks_{buf_ring_size};
  std::atomic<uint16_t> waiting_cnt_{0};
  std::vector<brpc::Socket *> waiting_batch_{buf_ring_size};

  std::atomic<bool> has_external_{true};

  std::vector<uint16_t> free_reg_fd_idx_;

  inline static size_t buf_length = sysconf(_SC_PAGESIZE);
  inline static size_t buf_ring_size = 1024;
};