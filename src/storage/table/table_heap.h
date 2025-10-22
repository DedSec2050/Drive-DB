#pragma once

#include <vector>
#include "src/storage/page/page.h"
#include "src/storage/buffer/buffer_pool.h"
#include "src/storage/table/tuple.h"  // Including all Value/Tuple types

namespace storage {

class TableHeap {
public:
    explicit TableHeap(uint32_t segment_id) : segment_id_(segment_id) {}

    // Read all rows from this table
    std::vector<std::vector<Value>> Scan(BufferPool &bp);

private:
    uint32_t segment_id_;
};

} // namespace storage
