#pragma once
#include "src/storage/page/page.h"
#include <fstream>
#include <string>
#include <unordered_map>
#include <mutex>

namespace storage {

class SegmentManager {
public:
    explicit SegmentManager(const std::string &base_dir);
    ~SegmentManager();

    Page read_page(const PageId &pid);
    void write_page(const Page &page);
    PageId allocate_page(uint32_t segment_id);
    void free_page(const PageId &pid);

private:
    std::string base_dir_;
    std::unordered_map<uint32_t, std::fstream> segments_;
    std::mutex mu_;

    std::fstream &get_segment(uint32_t segment_id);
};

} // namespace storage
