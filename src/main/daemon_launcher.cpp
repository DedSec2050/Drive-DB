#include "src/main/daemon_launcher.h"
#include "src/engine/engine.h"
#include "src/utils/logger.h"

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <iostream>
#include <mutex>
#include <thread>

// ---------------- Context ----------------
struct DaemonContext {
    std::atomic<bool> terminate{false};
    std::mutex mtx;
    std::condition_variable cv;
    bool finished = false;
};

static DaemonContext g_daemon_ctx;

// ---------------- Signal Handling ----------------
static void signal_handler(int sig) {
    if (sig == SIGTERM || sig == SIGINT) {
        g_daemon_ctx.terminate.store(true, std::memory_order_relaxed);
        g_daemon_ctx.cv.notify_all();
    }
}

// ---------------- Server Worker ----------------
static void server_worker(Engine &engine) {
    log(LogLevel::INFO, "Daemon server thread started");

    // Stub loop — later will delegate to src/server/Server
    while (!g_daemon_ctx.terminate.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        log(LogLevel::DEBUG, "Daemon heartbeat: engine alive");
    }

    {
        std::unique_lock<std::mutex> lk(g_daemon_ctx.mtx);
        g_daemon_ctx.finished = true;
    }
    g_daemon_ctx.cv.notify_all();

    log(LogLevel::INFO, "Daemon server thread exiting");
}

// ---------------- Lifecycle Entry ----------------
int start_daemon(Config &cfg) {
    log(LogLevel::INFO, "Starting daemon mode");

    // Register signals
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Initialize engine
    Engine engine(cfg);
    std::string err;
    if (!engine.init(err)) {
        log(LogLevel::ERROR, "Failed to initialize engine: " + err);
        return 1;
    }

    // Launch server thread
    std::thread srv_thread(server_worker, std::ref(engine));

    // Wait until termination or finished
    {
        std::unique_lock<std::mutex> lk(g_daemon_ctx.mtx);
        g_daemon_ctx.cv.wait(lk, [] {
            return g_daemon_ctx.finished || g_daemon_ctx.terminate.load(std::memory_order_relaxed);
        });
    }

    // Join worker thread
    if (srv_thread.joinable()) {
        srv_thread.join();
    }

    // Shutdown engine
    engine.shutdown();

    log(LogLevel::INFO, "Daemon mode shutdown complete");
    return 0;
}
