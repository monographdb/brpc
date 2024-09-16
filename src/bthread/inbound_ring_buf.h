#pragma once

#include <cstdint>

namespace brpc {
class Socket;
}

struct InboundRingBuf {
  InboundRingBuf() = default;

  InboundRingBuf(brpc::Socket *sock, int32_t bytes, uint16_t bid,
                 bool rearm = false)
      : sock_(sock), bytes_(bytes), buf_id_(bid), need_rearm_(rearm) {}

  InboundRingBuf(InboundRingBuf &&rhs)
      : sock_(rhs.sock_), bytes_(rhs.bytes_), buf_id_(rhs.buf_id_),
        need_rearm_(rhs.need_rearm_) {}

  InboundRingBuf &operator=(InboundRingBuf &&rhs) {
    if (this == &rhs) {
      return *this;
    }
    sock_ = rhs.sock_;
    bytes_ = rhs.bytes_;
    buf_id_ = rhs.buf_id_;
    need_rearm_ = rhs.need_rearm_;
    return *this;
  }

  brpc::Socket *sock_{nullptr};
  int32_t bytes_{0};
  uint16_t buf_id_{UINT16_MAX};
  bool need_rearm_{false};
};