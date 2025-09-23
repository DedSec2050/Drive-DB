#pragma once
#include "src/storage/page/page.h"
#include <cstdint>
#include <cstring>
#include <vector>

namespace storage {

struct RecordId {
    PageId pid;
    uint16_t slot;
};

struct Slot {
    uint16_t offset;
    uint16_t size;
};

struct HeapPage {
    PageId id;
    uint16_t free_start;
    uint16_t free_end;
    uint16_t slot_count;
    Slot slots[64]; // fixed slot directory
    char data[PAGE_SIZE - 512]; // rest of page

    HeapPage() {
        free_start = 0;
        free_end = sizeof(data);
        slot_count = 0;
    }

    bool insert(const char *buf, uint16_t len, RecordId &rid);
    bool update(uint16_t slot, const char *buf, uint16_t len);
    bool erase(uint16_t slot);
    std::vector<char> read(uint16_t slot) const;
};

} // namespace storage
