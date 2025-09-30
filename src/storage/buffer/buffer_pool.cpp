// src/storage/buffer/buffer_pool.cpp
#include "src/storage/buffer/buffer_pool.h"
#include <stdexcept>

using namespace storage;

BufferPool::BufferPool(size_t pool_size, SegmentManager &sm)
    : pool_size_(pool_size), sm_(sm) {}

uint64_t BufferPool::page_key(const PageId &pid) {
    return (static_cast<uint64_t>(pid.segment_id) << 32) | pid.page_number;
}

Frame* BufferPool::fetch_page(const PageId &pid, bool for_write) {
    std::lock_guard<std::mutex> lg(mu_);
    uint64_t key = page_key(pid);

    // if already loaded
    auto it = table_.find(key);
    if (it != table_.end()) {
        lru_list_.remove(pid);
        lru_list_.push_front(pid);
        it->second.pin_count++;
        return &it->second;
    }

    // need to evict if full
    evict_if_needed();

    // load page
    Frame frame;
    try {
        frame.page = sm_.read_page(pid);
    } catch (const std::out_of_range &) {
        // if missing, allocate new page
        frame.page.reset(pid, PageType::TABLE_HEAP);
    }
    frame.dirty = false;
    frame.pin_count = 1;

    lru_list_.push_front(pid);
    table_[key] = std::move(frame);
    return &table_[key];
}

Frame* BufferPool::fetch_or_allocate_page(const PageId &pid, bool for_write) {
    try {
        return fetch_page(pid, for_write);
    } catch (const std::out_of_range &) {
        // allocate a fresh page at this page_number
        PageId newpid = sm_.allocate_page(pid.segment_id);
        if (newpid.page_number != pid.page_number) {
            throw std::runtime_error("fetch_or_allocate_page: allocation mismatch");
        }
        return fetch_page(newpid, for_write);
    }
}

void BufferPool::unpin_page(Frame *frame, bool is_dirty) {
    std::lock_guard<std::mutex> lg(mu_);
    if (is_dirty) frame->dirty = true;
    frame->pin_count = std::max(0, frame->pin_count - 1);
}

void BufferPool::flush_page(Frame *frame) {
    if (frame->dirty) {
        sm_.write_page(frame->page);
        frame->dirty = false;
    }
}

void BufferPool::evict_if_needed() {
    if (table_.size() < pool_size_) return;

    // find victim from LRU
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        PageId victim = *it;
        uint64_t key = page_key(victim);
        Frame &f = table_[key];
        if (f.pin_count == 0) {
            flush_page(&f);
            table_.erase(key);
            lru_list_.remove(victim);
            return;
        }
    }
    throw std::runtime_error("BufferPool full: no evictable page");
}

PageId BufferPool::allocate_page(uint32_t segment_id) {
    PageId pid = sm_.allocate_page(segment_id);
    Frame f;
    f.page.reset(pid, PageType::TABLE_HEAP);
    f.dirty = true;
    f.pin_count = 1;
    table_[page_key(pid)] = f;
    lru_list_.push_front(pid);
    return pid;
}
