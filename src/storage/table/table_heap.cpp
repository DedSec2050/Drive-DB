#include "src/storage/table/table_heap.h"
#include "src/storage/buffer/buffer_pool.h"
#include "src/storage/page/heap_page.h"
#include "src/storage/table/tuple.h"

using namespace storage;

std::vector<std::vector<Value>> TableHeap::Scan(BufferPool &bp) {
    std::vector<std::vector<Value>> results;

    uint32_t segment_id = segment_id_;
    uint32_t page_no = 0;

    while (true) {
        PageId pid{segment_id, page_no};

        try {
            Frame* frame = bp.fetch_page(pid);

            HeapPage* hp = reinterpret_cast<HeapPage*>(frame->page.data);

            // Use your helper function
            auto records = hp->get_all_records();
            results.insert(results.end(), records.begin(), records.end());

            bp.unpin_page(frame, false);
        } catch (const std::out_of_range&) {
            break; // no more pages
        }

        page_no++;
    }

    return results;
}
