#include "src/engine/engine.h"
#include "src/utils/logger.h"

#include <filesystem>
#include <sstream>
#include <chrono>
#include <iostream>

namespace fs = std::filesystem;

Engine::Engine(const Config &cfg) : cfg_(cfg) {}

Engine::~Engine() {
    // ensure proper shutdown
    shutdown();
    join();
}

bool Engine::init(std::string &err) {
    try {
        if (!fs::exists(cfg_.data_dir)) {
            fs::create_directories(cfg_.data_dir);
            log(LogLevel::INFO, "created data_dir: " + cfg_.data_dir);
        }
    } catch (const std::exception &e) {
        err = std::string("failed to create data_dir: ") + e.what();
        return false;
    }

    // load catalog
    std::string catalog_path = cfg_.data_dir + "/catalog.meta";
    if (!catalog_.load_from_file(catalog_path, err)) {
        return false;
    }

    // ✅ init storage + executor
    segmgr_ = std::make_unique<storage::SegmentManager>(cfg_.data_dir);
    buffer_pool_ = std::make_unique<storage::BufferPool>(128, *segmgr_); // 128 frames default
    executor_ = std::make_unique<Executor>(catalog_, *buffer_pool_);

    log(LogLevel::INFO, "Engine initialized");
    return true;
}

void Engine::start_background() {
    bool expected = false;
    if (!bg_running_.compare_exchange_strong(expected, true)) {
        // already running
        return;
    }

    terminate_.store(false);
    bg_thread_ = std::thread(&Engine::background_loop, this);
    log(LogLevel::INFO, "Engine background thread started");
}

void Engine::shutdown() {
    bool was = terminate_.exchange(true);
    if (!was) {
        // notify background thread
        bg_cv_.notify_all();
    }
}

void Engine::join() {
    if (bg_thread_.joinable()) {
        // notify in case it's waiting
        bg_cv_.notify_all();
        bg_thread_.join();
        log(LogLevel::INFO, "Engine background thread joined");
    }
    bg_running_.store(false);
}

std::string Engine::execute_sql(const std::string &sql) {
    if (sql.rfind(".tables", 0) == 0) {
        auto tables = catalog_.list_tables();
        std::ostringstream ss;
        for (auto &t : tables) ss << t << '\n';
        return ss.str();
    }

    if (!executor_) {
        return "ERR: executor not initialized";
    }

    return executor_->execute(sql);
}

const catalog::Catalog& Engine::catalog() const {
    return catalog_;
}

void Engine::background_loop() {
    // Do light periodic maintenance (placeholder). Sleep and wake on shutdown.
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lk(bg_mu_);
    while (!terminate_.load()) {
        // Example maintenance: every 5s write a heartbeat log (production: checkpoints, GC, metrics)
        bg_cv_.wait_for(lk, 5s);
        if (terminate_.load()) break;
        log(LogLevel::DEBUG, "Engine background heartbeat");
        // (more maintenance work could be done here)
    }
    log(LogLevel::INFO, "Engine background loop exiting");
}
