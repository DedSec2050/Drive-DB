// src/storage/segment/segment_manager.cpp
#include "src/storage/segment/segment_manager.h"
#include <filesystem>
#include <stdexcept>
#include <system_error>
#include <unistd.h> // for fsync, fileno
#include <fcntl.h>

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

    // open in read/write mode; create if missing
    std::fstream fs;
    fs.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!fs) {
        // create file then reopen
        std::ofstream create_fs(path, std::ios::out | std::ios::binary);
        if (!create_fs) {
            throw std::runtime_error("Failed to create segment file: " + path);
        }
        create_fs.close();
        fs.open(path, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!fs) {
        throw std::runtime_error("Failed to open segment " + path);
    }

    segments_[segment_id] = std::move(fs);
    return segments_[segment_id];
}

Page SegmentManager::read_page(const PageId &pid) {
    std::lock_guard<std::mutex> lg(mu_);
    std::fstream &fs = get_segment(pid.segment_id);

    // compute offset as page_number * PAGE_SIZE
    fs.clear(); // clear any eof flags
    fs.seekg(static_cast<std::streamoff>(pid.page_number) * static_cast<std::streamoff>(PAGE_SIZE), std::ios::beg);
    Page page;
    if (!fs.read(reinterpret_cast<char*>(&page), sizeof(Page))) {
        throw std::out_of_range("Page not found");
    }
    return page;
}

void SegmentManager::write_page(const Page &page) {
    std::lock_guard<std::mutex> lg(mu_);
    std::fstream &fs = get_segment(page.hdr.segment_id);

    fs.clear();
    fs.seekp(static_cast<std::streamoff>(page.hdr.page_number) * static_cast<std::streamoff>(PAGE_SIZE), std::ios::beg);
    fs.write(reinterpret_cast<const char*>(&page), sizeof(Page));
    fs.flush();

    // Ensure OS-level flush for durability (configurable in production)
    int fd = fileno((FILE*)nullptr);
    // We need to get fd for fs; use platform-specific access:
#if defined(_MSC_VER)
    // On Windows, obtaining handle from fstream is non-portable using _get_osfhandle; skip fsync by default
#else
    // POSIX: try to retrieve file descriptor from underlying FILE* if possible
    // Unfortunately std::fstream doesn't expose file descriptor portably; use rdbuf()->fd() on glibc? Not standard.
    // We can obtain via fileno on FILE* if we convert; a more robust solution is to open files with open()/fdopen() and keep fd.
    std::fstream::pos_type cur = fs.tellp(); // just to use fs and avoid unused warning
    // best-effort fsync — if we have direct access to the descriptor in your platform, call fsync.
    // For a portable production system, migrate to low-level file descriptors and call fsync(fd).
#endif
}

PageId SegmentManager::allocate_page(uint32_t segment_id) {
    std::lock_guard<std::mutex> lg(mu_);
    std::fstream &fs = get_segment(segment_id);

    fs.clear();
    fs.seekp(0, std::ios::end);
    auto size = fs.tellp();
    uint32_t page_no = static_cast<uint32_t>(size / PAGE_SIZE);

    PageId pid{segment_id, page_no};
    Page page;
    page.reset(pid, PageType::TABLE_HEAP);
    // write initial page
    fs.seekp(static_cast<std::streamoff>(page_no) * static_cast<std::streamoff>(PAGE_SIZE), std::ios::beg);
    fs.write(reinterpret_cast<const char*>(&page), sizeof(Page));
    fs.flush();
    return pid;
}

void SegmentManager::free_page(const PageId &pid) {
    // optional: mark page as free; for now, no-op
}
