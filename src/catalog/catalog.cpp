#include "src/catalog/catalog.h"
#include "src/utils/logger.h"

#include <fstream>
#include <sstream>

using namespace catalog;

bool Catalog::create_table(const std::string &name, const std::vector<Column> &cols, std::string &err) {
    std::lock_guard<std::mutex> lg(mu_);
    if (tables_.count(name)) {
        err = "table already exists: " + name;
        return false;
    }
    Table t;
    t.name = name;
    t.columns = cols;
    tables_.emplace(name, std::move(t));
    return true;
}

std::optional<Table> Catalog::get_table(const std::string &name) const {
    std::lock_guard<std::mutex> lg(mu_);
    auto it = tables_.find(name);
    if (it == tables_.end()) return std::nullopt;
    return it->second;
}

std::vector<std::string> Catalog::list_tables() const {
    std::lock_guard<std::mutex> lg(mu_);
    std::vector<std::string> out;
    out.reserve(tables_.size());
    for (auto const &p : tables_) out.push_back(p.first);
    return out;
}

// Very small, robust line-based persistence.
// Format (line oriented):
// TABLE <name>
// COL <colname> <type>
// END
bool Catalog::load_from_file(const std::string &path, std::string &err) {
    std::ifstream ifs(path);
    if (!ifs) {
        // non-existent file is not an error here (empty catalog)
        log(LogLevel::INFO, "catalog file not found: " + path + " (starting fresh)");
        return true;
    }

    std::unordered_map<std::string, Table> tmp;
    std::string line;
    Table *cur = nullptr;
    while (std::getline(ifs, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        std::string tok;
        ss >> tok;
        if (tok == "TABLE") {
            std::string tname;
            ss >> tname;
            if (tname.empty()) {
                err = "malformed catalog: TABLE with empty name";
                return false;
            }
            tmp.emplace(tname, Table{tname, {}});
            cur = &tmp.at(tname);
        } else if (tok == "COL") {
            if (!cur) {
                err = "malformed catalog: COL without TABLE";
                return false;
            }
            std::string cname, ctype;
            ss >> cname >> ctype;
            if (cname.empty() || ctype.empty()) {
                err = "malformed catalog: COL line invalid";
                return false;
            }
            cur->columns.push_back(Column{cname, ctype});
        } else if (tok == "END") {
            cur = nullptr;
        } else {
            // unknown: skip but log
            log(LogLevel::WARN, "unknown catalog token: " + tok);
        }
    }

    {
        std::lock_guard<std::mutex> lg(mu_);
        tables_.swap(tmp);
    }
    log(LogLevel::INFO, "catalog loaded from " + path);
    return true;
}

bool Catalog::save_to_file(const std::string &path, std::string &err) {
    // write to temp file then atomic rename
    std::string tmp = path + ".tmp";
    std::ofstream ofs(tmp, std::ofstream::trunc);
    if (!ofs) {
        err = "failed opening catalog tmp file: " + tmp;
        return false;
    }

    {
        std::lock_guard<std::mutex> lg(mu_);
        for (auto const &p : tables_) {
            ofs << "TABLE " << p.second.name << '\n';
            for (auto const &c : p.second.columns) {
                ofs << "COL " << c.name << ' ' << c.type << '\n';
            }
            ofs << "END\n";
        }
    }
    ofs.close();
    if (!ofs) {
        err = "failed writing catalog tmp file: " + tmp;
        return false;
    }

    // atomic rename
    if (std::rename(tmp.c_str(), path.c_str()) != 0) {
        err = "failed to rename catalog tmp file";
        return false;
    }
    log(LogLevel::INFO, "catalog saved to " + path);
    return true;
}
