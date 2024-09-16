#pragma once

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <glog/logging.h>
#include <liburing.h>
#include <vector>

class UringBufferPool {
public:
  UringBufferPool(size_t pool_size, io_uring *ring) {
    mem_bulk_ = (char *)std::aligned_alloc(4096, 4096 * pool_size);

    std::vector<iovec> register_buf;
    register_buf.resize(pool_size);
    buf_pool_.resize(pool_size);

    for (size_t idx = 0; idx < pool_size; ++idx) {
      buf_pool_[idx] = idx;
      register_buf[idx].iov_base = mem_bulk_ + (idx * 4096);
      register_buf[idx].iov_len = 4096;
    }

    int ret = io_uring_register_buffers(ring, register_buf.data(),
                                        register_buf.size());
    if (ret != 0) {
      LOG(WARNING) << "Failed to register IO uring fixed buffers. errno: "
                   << ret;
      free(mem_bulk_);
      mem_bulk_ = nullptr;
      buf_pool_.clear();
    } else {
      LOG(INFO) << "IO uring fixed buffers registered, buffer count: "
                << pool_size;
    }
  }

  ~UringBufferPool() {
    if (mem_bulk_ != nullptr) {
      free(mem_bulk_);
    }
  }

  std::pair<char *, uint16_t> Get() {
    if (buf_pool_.empty()) {
      return {nullptr, UINT16_MAX};
    } else {
      uint16_t buf_idx = buf_pool_.back();
      buf_pool_.pop_back();
      return {mem_bulk_ + (buf_idx * 4096), buf_idx};
    }
  }

  void Recycle(uint16_t buf_idx) {
    buf_pool_.emplace_back(buf_idx);
  }

private:
  char *mem_bulk_{nullptr};
  std::vector<uint16_t> buf_pool_;
};