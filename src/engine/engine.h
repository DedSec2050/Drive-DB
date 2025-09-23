#pragma once

#include "src/cli/config.h"
#include "src/catalog/catalog.h"
#include "src/storage/segment/segment_manager.h"
#include "src/storage/buffer/buffer_pool.h"
#include "src/execution/executor.h"

#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>

class Engine {
public:
    explicit Engine(const Config &cfg);
    ~Engine();

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    bool init(std::string &err);
    void start_background();
    void shutdown();
    void join();

    std::string execute_sql(const std::string &sql);

    const catalog::Catalog& catalog() const;

private:
    void background_loop();

private:
    Config cfg_;
    catalog::Catalog catalog_;

    // NEW: storage + executor
    std::unique_ptr<storage::SegmentManager> segmgr_;
    std::unique_ptr<storage::BufferPool> buffer_pool_;
    std::unique_ptr<Executor> executor_;

    std::atomic<bool> terminate_{false};
    std::atomic<bool> bg_running_{false};
    std::thread bg_thread_;
    std::mutex bg_mu_;
    std::condition_variable bg_cv_;
};
