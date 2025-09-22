#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <optional>

namespace catalog {

struct Column {
    std::string name;
    std::string type; // simple textual type for now
};

struct Table {
    std::string name;
    std::vector<Column> columns;
};

class Catalog {
public:
    Catalog() = default;

    // thread-safe accessors
    bool create_table(const std::string &name, const std::vector<Column> &cols, std::string &err);
    std::optional<Table> get_table(const std::string &name) const;
    std::vector<std::string> list_tables() const;

    // persistence (simple file API; engine will call with data_dir/catalog.meta)
    bool load_from_file(const std::string &path, std::string &err);
    bool save_to_file(const std::string &path, std::string &err);

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, Table> tables_;
};

} // namespace catalog
