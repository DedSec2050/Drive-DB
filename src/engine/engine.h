#pragma once

#include "src/cli/config.h"
#include "src/catalog/catalog.h"

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

    // non-copyable
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // initialize resources (must be called before use)
    bool init(std::string &err);

    // start background workers (maintenance, checkpoint, etc.)
    void start_background();

    // request graceful shutdown (returns immediately)
    void shutdown();

    // block until background workers have terminated
    void join();

    // simple entry point for REPL: execute a SQL or meta-command
    // returns human-readable reply (or error string prefixed with "ERR: ")
    std::string execute_sql(const std::string &sql);

    // expose catalog for read-only debug usage
    const catalog::Catalog& catalog() const;

private:
    void background_loop();

private:
    Config cfg_;
    catalog::Catalog catalog_;

    std::atomic<bool> terminate_{false};
    std::atomic<bool> bg_running_{false};
    std::thread bg_thread_;
    std::mutex bg_mu_;
    std::condition_variable bg_cv_;
};
