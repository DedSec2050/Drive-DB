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
    // ensure data dir exists
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
    // Very small dispatcher: support meta-commands:
    // .tables  -> list tables
    // CREATE TABLE name (col type, ...)
    // For production you'd hook a parser and planner here.
    if (sql.rfind(".tables", 0) == 0) {
        auto tables = catalog_.list_tables();
        std::ostringstream ss;
        for (auto &t : tables) ss << t << '\n';
        return ss.str();
    }

    // basic "CREATE TABLE" parser (robust but minimal)
    std::string s = sql;
    // trim
    auto l = s.find_first_not_of(" \t\r\n");
    if (l == std::string::npos) return "";
    auto r = s.find_last_not_of(" \t\r\n");
    s = s.substr(l, r - l + 1);

    // case-insensitive check for CREATE TABLE
    std::string upper;
    upper.resize(s.size());
    std::transform(s.begin(), s.end(), upper.begin(), ::toupper);

    if (upper.rfind("CREATE TABLE", 0) == 0) {
        // naive parse: CREATE TABLE name (a T, b T);
        auto p1 = s.find('(');
        auto p2 = s.rfind(')');
        if (p1 == std::string::npos || p2 == std::string::npos || p2 <= p1) {
            return std::string("ERR: malformed CREATE TABLE");
        }
        // extract name between "CREATE TABLE" and '('
        std::string name_part = s.substr(strlen("CREATE TABLE"), p1 - strlen("CREATE TABLE"));
        // trim
        auto nl = name_part.find_first_not_of(" \t\r\n");
        if (nl == std::string::npos) return std::string("ERR: missing table name");
        auto nr = name_part.find_last_not_of(" \t\r\n");
        std::string tblname = name_part.substr(nl, nr - nl + 1);

        std::string cols_str = s.substr(p1 + 1, p2 - p1 - 1);
        std::vector<catalog::Column> cols;
        // split by commas
        std::istringstream colss(cols_str);
        std::string piece;
        while (std::getline(colss, piece, ',')) {
            // trim
            auto cl = piece.find_first_not_of(" \t\r\n");
            if (cl == std::string::npos) continue;
            auto cr = piece.find_last_not_of(" \t\r\n");
            std::string cp = piece.substr(cl, cr - cl + 1);
            // split by space: name type
            std::istringstream ps(cp);
            std::string cname, ctype;
            ps >> cname >> ctype;
            if (cname.empty() || ctype.empty()) {
                return std::string("ERR: malformed column definition: ") + cp;
            }
            cols.push_back({cname, ctype});
        }

        std::string err;
        if (!catalog_.create_table(tblname, cols, err)) {
            return std::string("ERR: ") + err;
        }
        // persist current catalog synchronously
        std::string catalog_path = cfg_.data_dir + "/catalog.meta";
        if (!catalog_.save_to_file(catalog_path, err)) {
            return std::string("ERR: failed to save catalog: ") + err;
        }
        return std::string("OK: table created: ") + tblname;
    }

    // unrecognized
    return std::string("ERR: unsupported or malformed command");
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
