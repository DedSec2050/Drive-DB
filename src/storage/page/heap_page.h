#pragma once

#include <vector>
#include "src/storage/table/tuple.h"

namespace storage {

struct HeapPage {
    // existing fields...
    uint16_t num_records;
    char payload[PAGE_PAYLOAD_SIZE - sizeof(uint16_t)];

    std::vector<std::vector<Value>> get_all_records() const {
        std::vector<std::vector<Value>> records;
        const char *ptr = payload;
        for (int i = 0; i < num_records; i++) {
            uint16_t len;
            std::memcpy(&len, ptr, sizeof(uint16_t));
            ptr += sizeof(uint16_t);

            std::vector<Value> row;
            // Assuming each tuple serialized as [len][bytes...]
            // You’ll need to deserialize based on your Value format:
            Value v(std::string(ptr, len));
            row.push_back(v);

            ptr += len;
            records.push_back(row);
        }
        return records;
    }
};

} // namespace storage
