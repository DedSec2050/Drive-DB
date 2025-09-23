// src/storage/segment/segment_manager.cpp
#include "src/storage/segment/segment_manager.h"
#include <filesystem>
#include <stdexcept>

using namespace storage;

SegmentManager::SegmentManager(const std::string &base_dir) : base_dir_(base_dir) {
    std::filesystem::create_directories(base_dir_);
}

SegmentManager::~SegmentManager() {
    for (auto &kv : segments_) {
        kv.second.close();
    }
}

std::fstream &SegmentManager::get_segment(uint32_t segment_id) {
    auto it = segments_.find(segment_id);
    if (it != segments_.end()) return it->second;

    std::string path = base_dir_ + "/seg_" + std::to_string(segment_id) + ".dat";
    std::fstream fs(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!fs.is_open()) {
        // create if missing
        fs.open(path, std::ios::out | std::ios::binary);
        fs.close();
        fs.open(path, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!fs.is_open()) {
        throw std::runtime_error("Failed to open segment " + path);
    }
    segments_[segment_id] = std::move(fs);
    return segments_[segment_id];
}

Page SegmentManager::read_page(const PageId &pid) {
    std::lock_guard<std::mutex> lg(mu_);
    std::fstream &fs = get_segment(pid.segment_id);
    fs.seekg(static_cast<std::streamoff>(pid.page_number) * PAGE_SIZE);
    Page page;
    if (!fs.read(reinterpret_cast<char*>(&page), sizeof(Page))) {
        throw std::out_of_range("Page not found");
    }
    return page;
}

void SegmentManager::write_page(const Page &page) {
    std::lock_guard<std::mutex> lg(mu_);
    std::fstream &fs = get_segment(page.id.segment_id);
    fs.seekp(static_cast<std::streamoff>(page.id.page_number) * PAGE_SIZE);
    fs.write(reinterpret_cast<const char*>(&page), sizeof(Page));
    fs.flush();
}

PageId SegmentManager::allocate_page(uint32_t segment_id) {
    std::lock_guard<std::mutex> lg(mu_);
    std::fstream &fs = get_segment(segment_id);
    fs.seekp(0, std::ios::end);
    auto size = fs.tellp();
    uint32_t page_no = static_cast<uint32_t>(size / PAGE_SIZE);

    PageId pid{segment_id, page_no};
    Page page;
    page.reset(pid, PageType::TABLE_HEAP);
    write_page(page);
    return pid;
}

void SegmentManager::free_page(const PageId &pid) {
    // optional: mark page as free; for now, no-op
}
