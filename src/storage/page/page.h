#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include <stdexcept>

namespace storage {

constexpr size_t PAGE_SIZE = 4096;

enum class PageType : uint16_t {
    INVALID = 0,
    TABLE_HEAP = 1,
    INDEX_INTERNAL = 2,
    INDEX_LEAF = 3
};

struct PageId {
    uint32_t segment_id;
    uint32_t page_number;
    bool operator==(const PageId &o) const {
        return segment_id == o.segment_id && page_number == o.page_number;
    }
};

struct Page {
    PageId id;
    PageType type;
    uint32_t lsn;
    char data[PAGE_SIZE - sizeof(PageId) - sizeof(PageType) - sizeof(uint32_t)];

    void reset(PageId pid, PageType t) {
        id = pid;
        type = t;
        lsn = 0;
        std::memset(data, 0, sizeof(data));
    }
};

} // namespace storage
