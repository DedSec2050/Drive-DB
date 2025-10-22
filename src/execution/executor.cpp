#include "src/execution/executor.h"
#include "src/storage/table/tuple.h"     // tuple include
#include "src/utils/logger.h"            // optional logger
#include <algorithm>
#include <sstream>
#include <cctype>
#include <cstring>

//
// ===============================================================
//                  HELPER FUNCTIONS (utilities)
// ===============================================================
//

// Trim whitespace from both ends
static inline std::string trim(const std::string &s) {
    auto a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    auto b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

// Split CSV-style strings into values (supports quoted strings)
static inline std::vector<std::string> split_csv(const std::string &s) {
    std::vector<std::string> out;
    std::string cur;
    bool in_quotes = false;

    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '"') {
            in_quotes = !in_quotes;
            continue;
        }
        if (c == ',' && !in_quotes) {
            out.push_back(trim(cur));
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) out.push_back(trim(cur));
    return out;
}

// Map a table name to a segment id (simple hash)
static uint32_t table_to_segment(const std::string &tname) {
    std::hash<std::string> h;
    return static_cast<uint32_t>(h(tname) & 0xFFFFFFFFu);
}

// Read/write the "used bytes" header from page.data
static uint32_t read_page_used_bytes(const storage::Page &p) {
    uint32_t used = 0;
    if (sizeof(p.data) >= sizeof(uint32_t)) {
        std::memcpy(&used, p.data, sizeof(uint32_t));
    }
    return used;
}

static void write_page_used_bytes(storage::Page &p, uint32_t used) {
    if (sizeof(p.data) >= sizeof(uint32_t)) {
        std::memcpy(p.data, &used, sizeof(uint32_t));
    }
}

//
// ===============================================================
//                  EXECUTOR IMPLEMENTATION
// ===============================================================
//

Executor::Executor(catalog::Catalog &catalog, storage::BufferPool &bp)
    : catalog_(catalog), bp_(bp) {}

std::string Executor::execute(const std::string &sql) {
    std::string s = trim(sql);
    if (s.empty()) return "";

    // Convert to uppercase for command detection
    std::string upper(s.size(), '\0');
    std::transform(s.begin(), s.end(), upper.begin(),
                   [](unsigned char c){ return std::toupper(c); });

    if (upper.rfind("CREATE TABLE", 0) == 0) return handle_create_table(s);
    if (upper.rfind("INSERT INTO", 0) == 0) return handle_insert(s);
    if (upper.rfind("SELECT", 0) == 0)       return handle_select(s);
    if (upper.rfind("UPDATE", 0) == 0)       return handle_update(s);
    if (upper.rfind("DELETE", 0) == 0)       return handle_delete(s);

    return "ERR: unsupported command";
}

//
// ------------------------- INSERT ------------------------------
//
std::string Executor::handle_insert(const std::string &sql) {
    // Parse SQL: INSERT INTO <table> VALUES (v1, v2, ...)
    size_t pos_into   = sql.find("INTO");
    size_t pos_values = sql.find("VALUES", pos_into);
    if (pos_into == std::string::npos || pos_values == std::string::npos)
        return "ERR: malformed INSERT";

    std::string tbl = trim(sql.substr(pos_into + 4, pos_values - (pos_into + 4)));
    // strip optional column list
    auto paren = tbl.find('(');
    if (paren != std::string::npos) tbl = trim(tbl.substr(0, paren));

    // Extract values inside parentheses
    size_t p1 = sql.find('(', pos_values);
    size_t p2 = sql.rfind(')');
    if (p1 == std::string::npos || p2 == std::string::npos || p2 <= p1)
        return "ERR: malformed INSERT values";

    std::string vals = sql.substr(p1 + 1, p2 - p1 - 1);
    auto pieces = split_csv(vals);

    // Lookup table schema
    auto maybe = catalog_.get_table(tbl);
    if (!maybe) return "ERR: unknown table " + tbl;
    const catalog::Table &table = *maybe;

    if (pieces.size() != table.columns.size()) {
        return "ERR: column count mismatch: expected " +
               std::to_string(table.columns.size());
    }

    // ----------------------------------------------
    // Build Tuple from parsed values
    // ----------------------------------------------
    std::vector<storage::Value> vals_vec;
    vals_vec.reserve(pieces.size());

    for (size_t i = 0; i < pieces.size(); i++) {
        std::string v = trim(pieces[i]);
        if (v.size() >= 2 && v.front() == '"' && v.back() == '"')
            v = v.substr(1, v.size() - 2);

        const auto &col = table.columns[i];
        std::string type_upper = col.type;
        std::transform(type_upper.begin(), type_upper.end(), type_upper.begin(), ::toupper);

        if (type_upper == "INT" || type_upper == "INTEGER")
            vals_vec.emplace_back(std::stoi(v));
        else
            vals_vec.emplace_back(v);
    }

    storage::Tuple tuple(std::move(vals_vec));
    auto payload = tuple.serialize();

    // ----------------------------------------------
    // Write to pages
    // ----------------------------------------------
    uint32_t seg = table_to_segment(tbl);
    uint32_t page_no = 0;
    bool written = false;

    while (true) {
        storage::PageId pid{seg, page_no};
        try {
            storage::Frame *frame = bp_.fetch_page(pid, true);
            uint32_t used = read_page_used_bytes(frame->page);

            uint32_t rec_len = static_cast<uint32_t>(payload.size());
            uint32_t need = 4 + rec_len;  // record header + tuple data
            size_t available = sizeof(frame->page.data) - used;

            if (need + 4 <= available) {
                // Append record
                uint32_t offset = used + 4;
                std::memcpy(frame->page.data + offset, &rec_len, 4);
                std::memcpy(frame->page.data + offset + 4, payload.data(), rec_len);

                used += need;
                write_page_used_bytes(frame->page, used);

                bp_.unpin_page(frame, true);
                written = true;
                break;
            } else {
                bp_.unpin_page(frame, false);
                page_no++;
                continue;
            }
        } catch (const std::out_of_range &) {
            // Allocate new page if not exist
            storage::PageId newpid = bp_.allocate_page(seg);
            storage::Frame *frame  = bp_.fetch_page(newpid, true);

            uint32_t rec_len = static_cast<uint32_t>(payload.size());
            std::memcpy(frame->page.data + 4, &rec_len, 4);
            std::memcpy(frame->page.data + 8, payload.data(), rec_len);

            uint32_t used = 4 + rec_len;
            write_page_used_bytes(frame->page, used);

            bp_.unpin_page(frame, true);
            written = true;
            break;
        }
    }

    return written ? "OK: 1 row inserted"
                   : "ERR: failed to write row";
}

//
// ------------------------- SELECT ------------------------------
//
std::string Executor::handle_select(const std::string &sql) {
    // Only supports: SELECT * FROM <table>
    std::string upper = sql;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    size_t pos_from = upper.find("FROM");
    if (pos_from == std::string::npos) return "ERR: malformed SELECT";

    std::string tbl = trim(sql.substr(pos_from + 4));
    auto wherepos = tbl.find("WHERE");
    if (wherepos != std::string::npos) tbl = trim(tbl.substr(0, wherepos));
    if (!tbl.empty() && tbl.back() == ';') tbl.pop_back();
    tbl = trim(tbl);

    auto maybe = catalog_.get_table(tbl);
    if (!maybe) return "ERR: unknown table " + tbl;
    const catalog::Table &table = *maybe;

    uint32_t seg = table_to_segment(tbl);
    std::ostringstream out;
    uint32_t page_no = 0;

    // Scan all pages
    while (true) {
        storage::PageId pid{seg, page_no};
        storage::Frame *frame = nullptr;

        try {
            frame = bp_.fetch_page(pid, false);
        } catch (...) {
            break;
        }

        uint32_t used = read_page_used_bytes(frame->page);
        uint32_t offset = 4;

        while (offset + 4 <= used + 4) {
            const char *ptr = frame->page.data + offset;
            uint32_t rec_len = 0;
            std::memcpy(&rec_len, ptr, 4);
            ptr += 4;

            if (rec_len == 0 || offset + 4 + rec_len > sizeof(frame->page.data))
                break;

            const char *tuple_ptr = ptr;
            storage::Tuple tup = storage::Tuple::deserialize(tuple_ptr);

            // Print row
            for (size_t i = 0; i < tup.values().size() && i < table.columns.size(); ++i) {
                out << table.columns[i].name << "=" << tup.values()[i].to_string();
                if (i + 1 < tup.values().size()) out << ", ";
            }
            out << "\n";

            offset += 4 + rec_len;
        }

        bp_.unpin_page(frame, false);
        page_no++;
    }

    std::string res = out.str();
    return res.empty() ? "OK: 0 rows" : res;
}

//
// ---------------------- CREATE TABLE ---------------------------
//
std::string Executor::handle_create_table(const std::string &sql) {
    // Parse CREATE TABLE <name> (col type, ...)
    std::string s = trim(sql);
    std::string upper(s.size(), '\0');
    std::transform(s.begin(), s.end(), upper.begin(), ::toupper);
    if (upper.rfind("CREATE TABLE", 0) != 0) return "ERR: malformed CREATE TABLE";

    auto p1 = s.find('(');
    auto p2 = s.rfind(')');
    if (p1 == std::string::npos || p2 == std::string::npos || p2 <= p1)
        return "ERR: malformed CREATE TABLE";

    std::string name_part = s.substr(strlen("CREATE TABLE"), p1 - strlen("CREATE TABLE"));
    auto nl = name_part.find_first_not_of(" \t\r\n");
    auto nr = name_part.find_last_not_of(" \t\r\n");
    if (nl == std::string::npos) return "ERR: missing table name";
    std::string tblname = name_part.substr(nl, nr - nl + 1);

    // Parse columns
    std::string cols_str = s.substr(p1 + 1, p2 - p1 - 1);
    std::vector<catalog::Column> cols;
    std::istringstream colss(cols_str);
    std::string piece;
    while (std::getline(colss, piece, ',')) {
        auto cl = piece.find_first_not_of(" \t\r\n");
        if (cl == std::string::npos) continue;
        auto cr = piece.find_last_not_of(" \t\r\n");
        std::string cp = piece.substr(cl, cr - cl + 1);

        std::istringstream ps(cp);
        std::string cname, ctype;
        ps >> cname >> ctype;
        if (cname.empty() || ctype.empty())
            return "ERR: malformed column: " + cp;

        cols.push_back({cname, ctype});
    }

    // Create table in catalog
    std::string err;
    if (!catalog_.create_table(tblname, cols, err)) return "ERR: " + err;

    // Persist catalog
    std::string path = "./data/catalog.meta";
    if (!catalog_.save_to_file(path, err))
        return "ERR: failed to save catalog: " + err;

    return "OK: table created: " + tblname;
}

//
// ---------------------- NOT IMPLEMENTED ------------------------
//
std::string Executor::handle_update(const std::string &) {
    return "ERR: UPDATE not implemented in this version";
}

std::string Executor::handle_delete(const std::string &) {
    return "ERR: DELETE not implemented in this version";
}
