#pragma once

#include <cstdint>

struct EpollEntry {
  EpollEntry() = default;

  EpollEntry(uint64_t sock, uint32_t events, const void *attr)
      : socket_id_(sock), events_(events), attr_(attr) {}

  EpollEntry(EpollEntry &&rhs)
      : socket_id_(rhs.socket_id_), events_(rhs.events_), attr_(rhs.attr_) {}

  EpollEntry &operator=(EpollEntry &&rhs) {
    if (this != &rhs) {
      socket_id_ = rhs.socket_id_;
      events_ = rhs.events_;
      attr_ = rhs.attr_;
    }
    return *this;
  }

  EpollEntry &operator=(const EpollEntry &rhs) {
    if (this != &rhs) {
      socket_id_ = rhs.socket_id_;
      events_ = rhs.events_;
      attr_ = rhs.attr_;
    }
    return *this;
  }

  uint64_t socket_id_{0};
  uint32_t events_{0};
  const void *attr_{nullptr};
};