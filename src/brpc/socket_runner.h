#pragma once

#include "brpc/input_messenger.h"
#include "brpc/socket.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"

enum struct SocketStatus { Busy = 0, Closed };

class SocketRunner {
public:
  SocketRunner(brpc::Socket *sock) : socket_(sock) {}

  void Notify() {
    std::unique_lock<bthread::Mutex> lk(mux_);
    cv_.notify_one();
  }

  SocketStatus Status() const {
    return status_.load(std::memory_order_relaxed);
  }

  void Close() {
    status_.store(SocketStatus::Closed, std::memory_order_relaxed);
  }

private:
  brpc::Socket *const socket_;
  bthread::Mutex mux_;
  bthread::ConditionVariable cv_;
  std::atomic<SocketStatus> status_{SocketStatus::Busy};

  friend class brpc::Socket;
};