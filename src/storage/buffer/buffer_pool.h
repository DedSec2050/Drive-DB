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
        std::list<PageId> lru_list_;
        std::unordered_map<uint64_t, Frame> table_;
        std::mutex mu_;

        void evict_if_needed();
        static uint64_t page_key(const PageId &pid);
    };

} // namespace storage
