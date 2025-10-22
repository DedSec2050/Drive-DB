// src/storage/buffer/buffer_pool.cpp
#include "src/storage/buffer/buffer_pool.h"
#include <stdexcept>
#include <cassert>

using namespace storage;

uint64_t BufferPool::page_key(const PageId &pid) {
    return (static_cast<uint64_t>(pid.segment_id) << 32) | pid.page_number;
}

BufferPool::BufferPool(size_t pool_size, SegmentManager &sm)
    : pool_size_(pool_size), sm_(sm) {}

void BufferPool::touch_locked(const PageId &pid) {
    uint64_t key = page_key(pid);
    auto it = lru_pos_.find(key);
    if (it != lru_pos_.end()) {
        lru_list_.erase(it->second);
    }
    lru_list_.push_front(key);
    lru_pos_[key] = lru_list_.begin();
}

void BufferPool::evict_if_needed_locked() {
    if (table_.size() < pool_size_) return;

    // find victim from LRU tail
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        uint64_t key = *it;
        auto f_it = table_.find(key);
        if (f_it == table_.end()) continue; // should not happen, but guard
        Frame &f = f_it->second;
        if (f.pin_count == 0) {
            // flush and remove
            if (f.dirty) {
                // flush outside lock? we flush while holding lock for simplicity here.
                sm_.write_page(f.page);
                f.dirty = false;
            }
            // remove from structures
            auto lit = lru_pos_.find(key);
            if (lit != lru_pos_.end()) {
                lru_list_.erase(lit->second);
                lru_pos_.erase(lit);
            }
            table_.erase(f_it);
            return;
        }
    }
    throw std::runtime_error("BufferPool full: no evictable page");
}

Frame* BufferPool::fetch_page(const PageId &pid, bool for_write) {
    uint64_t key = page_key(pid);

    {   // scope lock for metadata
        std::lock_guard<std::mutex> lg(mu_);
        auto it = table_.find(key);
        if (it != table_.end()) {
            // found in cache
            it->second.pin_count++;
            touch_locked(pid);
            return &it->second;
        }

        // ensure space for insertion
        evict_if_needed_locked();

        // create placeholder Frame in map so pointer remains stable while we unlock
        Frame placeholder;
        placeholder.dirty = false;
        placeholder.pin_count = 1;
        // page will be filled below after reading from disk
        table_.emplace(key, std::move(placeholder));
        // record LRU position
        lru_list_.push_front(key);
        lru_pos_[key] = lru_list_.begin();
    }

    // Now load page content from disk (do this outside lock to avoid blocking others).
    Page p;
    bool loaded = true;
    try {
        p = sm_.read_page(pid);
    } catch (const std::out_of_range &) {
        // page missing => new blank page
        p.reset(pid, PageType::TABLE_HEAP);
        loaded = false;
    }

    // assign loaded page into map entry
    {
        std::lock_guard<std::mutex> lg(mu_);
        auto it = table_.find(key);
        assert(it != table_.end());
        it->second.page = std::move(p);
        it->second.dirty = !loaded; // if newly allocated in memory, mark dirty so it'll be written
        it->second.pin_count = 1;   // ensure pin_count is 1
        return &it->second;
    }
}

Frame* BufferPool::fetch_or_allocate_page(const PageId &pid, bool for_write) {
    // The semantic: if page exists, return; otherwise allocate a fresh page at that page_number
    // We'll try fetch_page first — if it gives a blank page (created in memory) we accept it.
    try {
        return fetch_page(pid, for_write);
    } catch (...) {
        // fallback: allocate at requested segment (this shouldn't normally happen with current fetch_page)
        std::lock_guard<std::mutex> lg(mu_);
        evict_if_needed_locked();
        PageId newpid = sm_.allocate_page(pid.segment_id);
        if (newpid.page_number != pid.page_number) {
            throw std::runtime_error("fetch_or_allocate_page: allocation mismatch");
        }
        uint64_t key = page_key(newpid);
        Frame f;
        f.page.reset(newpid, PageType::TABLE_HEAP);
        f.dirty = true;
        f.pin_count = 1;
        table_[key] = std::move(f);
        lru_list_.push_front(key);
        lru_pos_[key] = lru_list_.begin();
        return &table_[key];
    }
}

void BufferPool::unpin_page(Frame *frame, bool is_dirty) {
    std::lock_guard<std::mutex> lg(mu_);
    if (!frame) return;
    if (is_dirty) frame->dirty = true;
    frame->pin_count = std::max(0, frame->pin_count - 1);
}

void BufferPool::flush_page(Frame *frame) {
    if (!frame) return;
    std::lock_guard<std::mutex> lg(mu_);
    if (frame->dirty) {
        sm_.write_page(frame->page);
        frame->dirty = false;
    }
}

PageId BufferPool::allocate_page(uint32_t segment_id) {
    // allocate a new page on disk (sm_ will append) then insert into bufferpool
    PageId pid = sm_.allocate_page(segment_id);

    std::lock_guard<std::mutex> lg(mu_);
    evict_if_needed_locked();

    uint64_t key = page_key(pid);
    Frame f;
    f.page.reset(pid, PageType::TABLE_HEAP);
    f.dirty = true;   // newly allocated, must be persisted (sm_.allocate_page already created on disk, but we mark dirty in memory)
    f.pin_count = 1;
    table_[key] = std::move(f);
    lru_list_.push_front(key);
    lru_pos_[key] = lru_list_.begin();
    return pid;
}
