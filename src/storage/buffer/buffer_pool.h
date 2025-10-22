#pragma once
#include "src/storage/page/page.h"
#include "src/storage/segment/segment_manager.h"
#include <list>
#include <unordered_map>
#include <mutex>

namespace storage {

    struct Frame {
        Page page;
        bool dirty;
        int pin_count;
    };

    class BufferPool {
    public:
        PageId allocate_page(uint32_t segment_id);
        
        BufferPool(size_t pool_size, SegmentManager &sm);
        Frame* fetch_page(const PageId &pid, bool for_write = false);
        Frame* fetch_or_allocate_page(const PageId &pid, bool for_write = false);
        void unpin_page(Frame *frame, bool is_dirty);
        void flush_page(Frame *frame);

    private:
        size_t pool_size_;
        SegmentManager &sm_;

        // LRU: we store keys (uint64_t) in list and keep iterators for O(1) updates.
        std::list<uint64_t> lru_list_;
        std::unordered_map<uint64_t, Frame> table_;
        std::unordered_map<uint64_t, std::list<uint64_t>::iterator> lru_pos_;

        std::mutex mu_;

        void evict_if_needed_locked(); // expects mu_ held
        static uint64_t page_key(const PageId &pid);
        void touch_locked(const PageId &pid); // move to front; expects mu_ held
    };

} // namespace storage
