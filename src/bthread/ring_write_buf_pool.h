#pragma once

#include <cstdint>
#include <cstdlib>
#include <butil/logging.h>
#ifdef IO_URING_ENABLED
#include <liburing.h>
#endif
#include <vector>

#ifdef IO_URING_ENABLED
class RingWriteBufferPool {
public:
  RingWriteBufferPool(size_t pool_size, io_uring *ring) {
    mem_bulk_ = (char *)std::aligned_alloc(buf_length, buf_length * pool_size);

    std::vector<iovec> register_buf;
    register_buf.resize(pool_size);
    buf_pool_.resize(pool_size);

    for (size_t idx = 0; idx < pool_size; ++idx) {
      buf_pool_[idx] = idx;
      register_buf[idx].iov_base = mem_bulk_ + (idx * buf_length);
      register_buf[idx].iov_len = buf_length;
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

  ~RingWriteBufferPool() {
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
      return {mem_bulk_ + (buf_idx * buf_length), buf_idx};
    }
  }

  const char *GetBuf(uint16_t buf_idx) const {
    return mem_bulk_ + (buf_idx * buf_length);
  }

  void Recycle(uint16_t buf_idx) { buf_pool_.emplace_back(buf_idx); }

  inline static size_t buf_length = sysconf(_SC_PAGESIZE);

private:
  char *mem_bulk_{nullptr};
  std::vector<uint16_t> buf_pool_;
};
#endif