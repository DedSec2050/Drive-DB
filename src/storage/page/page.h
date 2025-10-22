#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <stdexcept>
#include <type_traits>

namespace storage {

constexpr size_t PAGE_SIZE = 4096;

#pragma pack(push, 1)  // <--- PACK STRUCT (no padding)
struct PageHeader {
    uint32_t segment_id;   // 4 bytes
    uint32_t page_number;  // 4 bytes
    uint16_t type;         // 2 bytes
    uint32_t lsn;          // 4 bytes
    uint8_t reserved[2];   // pad manually to reach 16
};
#pragma pack(pop)

static_assert(sizeof(PageHeader) == 16, "PageHeader must be 16 bytes");

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

constexpr size_t PAGE_HEADER_SIZE = sizeof(PageHeader);
constexpr size_t PAGE_PAYLOAD_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;

struct Page {
    PageHeader hdr;
    char data[PAGE_PAYLOAD_SIZE];

    void reset(const PageId &pid, PageType t) {
        hdr.segment_id = pid.segment_id;
        hdr.page_number = pid.page_number;
        hdr.type = static_cast<uint16_t>(t);
        hdr.lsn = 0;
        hdr.reserved[0] = hdr.reserved[1] = 0;
        std::memset(data, 0, sizeof(data));
    }

    PageId id() const { return PageId{hdr.segment_id, hdr.page_number}; }
    PageType type() const { return static_cast<PageType>(hdr.type); }
};

static_assert(sizeof(Page) == PAGE_SIZE, "Page size mismatch with PAGE_SIZE");

} // namespace storage
