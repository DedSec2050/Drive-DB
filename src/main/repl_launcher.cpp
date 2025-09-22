#include "src/main/repl_launcher.h"
#include "src/utils/logger.h"
#include "src/engine/engine.h"

#include <atomic>
#include <csignal>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <condition_variable>

static std::atomic<bool> g_terminate{false};
static std::mutex g_mtx;
static std::condition_variable g_cv;

// signal handler sets atomic flag and notifies
static void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        g_terminate.store(true);
        g_cv.notify_all();
    }
}

int start_repl(Config &cfg) {
    log(LogLevel::INFO, "Starting REPL mode (foreground)");

    // Setup signals (store previous handlers if needed)
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Construct engine
    Engine engine(cfg);
    std::string err;
    if (!engine.init(err)) {
        log(LogLevel::ERROR, std::string("Engine init failed: ") + err);
        return 1;
    }
    engine.start_background();

    // REPL worker runs on its own thread (keeps main responsive to signals)
    std::atomic<bool> repl_done{false};
    std::thread repl_thread([&]{
        log(LogLevel::INFO, "REPL thread started");
        std::string line;
        while (!g_terminate.load(std::memory_order_relaxed)) {
            std::cout << "boltd> " << std::flush;
            if (!std::getline(std::cin, line)) {
                // EOF (Ctrl+D)
                log(LogLevel::INFO, "REPL EOF received");
                break;
            }
            // trim leading/trailing
            auto l = line.find_first_not_of(" \t\r\n");
            if (l == std::string::npos) continue;
            auto r = line.find_last_not_of(" \t\r\n");
            std::string cmd = line.substr(l, r - l + 1);

            if (cmd == "exit" || cmd == "quit" || cmd == ":quit" || cmd == ":exit") {
                log(LogLevel::INFO, "Exit command received from REPL");
                g_terminate.store(true);
                break;
            }
            if (cmd == ":help") {
                std::cout << "Commands: :help :quit :backup :stats | exit | quit\n";
                continue;
            }

            // dispatch to engine
            std::string out = engine.execute_sql(cmd);
            if (!out.empty()) std::cout << out << std::endl;
        }
        repl_done.store(true);
        g_cv.notify_all();
        log(LogLevel::INFO, "REPL thread exiting");
    });

    // main thread waits for termination signal
    {
        std::unique_lock<std::mutex> lk(g_mtx);
        g_cv.wait(lk, [&]{ return g_terminate.load() || repl_done.load(); });
    }

    // shutdown engine and join worker
    log(LogLevel::INFO, "Shutting down engine from REPL launcher");
    engine.shutdown();
    engine.join();

    if (repl_thread.joinable()) repl_thread.join();

    log(LogLevel::INFO, "REPL mode shutdown complete");
    return 0;
}
