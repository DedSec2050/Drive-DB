// src/execution/executor.cpp
#include "src/execution/executor.h"
#include "src/utils/logger.h"// if you have a separate header; else use types declared in buffer/page headers
#include <algorithm>
#include <sstream>
#include <cctype>

static inline std::string trim(const std::string &s) {
    auto a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    auto b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

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

static uint32_t table_to_segment(const std::string &tname) {
    std::hash<std::string> h;
    return static_cast<uint32_t>(h(tname) & 0xFFFFFFFFu);
}

Executor::Executor(catalog::Catalog &catalog, storage::BufferPool &bp)
    : catalog_(catalog), bp_(bp) {}

std::string Executor::execute(const std::string &sql) {
    std::string s = trim(sql);
    if (s.empty()) return "";
    // simple uppercase for command detection
    std::string upper(s.size(), '\0');
    std::transform(s.begin(), s.end(), upper.begin(), [](unsigned char c){ return std::toupper(c); });

    if (upper.rfind("CREATE TABLE", 0) == 0) return handle_create_table(s);
    if (upper.rfind("INSERT INTO", 0) == 0) return handle_insert(s);
    if (upper.rfind("SELECT", 0) == 0) return handle_select(s);
    if (upper.rfind("UPDATE", 0) == 0) return handle_update(s);
    if (upper.rfind("DELETE", 0) == 0) return handle_delete(s);

    return std::string("ERR: unsupported command");
}

//
// Very pragmatic on-disk format for rows:
// Each page.data begins with a u32 used_bytes at offset 0 (we store in first 4 bytes of Page.data).
// Records are appended as:
// [u32 rec_len] [u8 rec_payload...] repeated.
// Rec payload serialized as: for each column: [u32 len][bytes] (no type encoding).
//
// This is intentionally simple: it allows variable-length columns and easy parsing.
// NOTE: This implementation stores one page per record (simple) if needed; we try to append pages.
//
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

std::string Executor::handle_insert(const std::string &sql) {
    // Very small parser:
    // INSERT INTO <table> VALUES (v1, v2, ...)
    size_t pos_into = sql.find("INTO");
    if (pos_into == std::string::npos) return "ERR: malformed INSERT";
    size_t pos_values = sql.find("VALUES", pos_into);
    if (pos_values == std::string::npos) return "ERR: malformed INSERT";
    std::string tbl = trim(sql.substr(pos_into + 4, pos_values - (pos_into + 4)));
    // remove optional table columns list (not supported)
    auto paren = tbl.find('(');
    if (paren != std::string::npos) tbl = trim(tbl.substr(0, paren));

    size_t p1 = sql.find('(', pos_values);
    size_t p2 = sql.rfind(')');
    if (p1 == std::string::npos || p2 == std::string::npos || p2 <= p1) return "ERR: malformed INSERT values";
    std::string vals = sql.substr(p1 + 1, p2 - p1 - 1);
    auto pieces = split_csv(vals);

    // get table schema
    auto maybe = catalog_.get_table(tbl);
    if (!maybe) return std::string("ERR: unknown table ") + tbl;
    const catalog::Table &table = *maybe;
    if (pieces.size() != table.columns.size()) {
        return std::string("ERR: column count mismatch: expected ") + std::to_string(table.columns.size());
    }

    // serialize record
    std::vector<uint8_t> payload;
    for (auto &val : pieces) {
        std::string v = trim(val);
        // strip optional surrounding quotes
        if (v.size() >= 2 && v.front() == '"' && v.back() == '"') v = v.substr(1, v.size()-2);
        uint32_t l = static_cast<uint32_t>(v.size());
        uint8_t buf4[4];
        std::memcpy(buf4, &l, 4);
        payload.insert(payload.end(), buf4, buf4 + 4);
        payload.insert(payload.end(), v.begin(), v.end());
    }

    // choose segment id for table
    uint32_t seg = table_to_segment(tbl);

    // simple append strategy: try to open last page number and append record if space, else allocate new page
    // We'll iterate page numbers starting 0,1,... until read_page throws out_of_range, then use that
    uint32_t page_no = 0;
    storage::Page page;
    bool written = false;
    while (true) {
        storage::PageId pid{seg, page_no};
        try {
            storage::Frame *frame = bp_.fetch_page(pid, true);
            // read used
            uint32_t used = read_page_used_bytes(frame->page);
            // compute needed space: 4 bytes record_len + payload
            uint32_t need = static_cast<uint32_t>(4 + payload.size());
            size_t available = sizeof(frame->page.data) - used;
            if (need + 4 /* some margin */ <= available) {
                // append: write record_len then bytes at data[used + 4]
                uint32_t offset = used + 4; // first 4 bytes reserved for used_bytes
                uint32_t rec_len = static_cast<uint32_t>(payload.size());
                std::memcpy(frame->page.data + offset, &rec_len, 4);
                std::memcpy(frame->page.data + offset + 4, payload.data(), payload.size());
                // update used
                used += (4 + rec_len);
                write_page_used_bytes(frame->page, used);
                bp_.unpin_page(frame, true);
                written = true;
                break;
            } else {
                // not enough space on this page
                bp_.unpin_page(frame, false);
                page_no++;
                continue;
            }
        } catch (const std::out_of_range &) {
            // page not exists: create/allocate a new page and write
            try {
                storage::PageId newpid = bp_.allocate_page(seg);
                storage::Frame *frame = bp_.fetch_page(newpid, true);
 // but sm_ is private in BufferPool - can't access
            } catch (...) {
                // We cannot access SegmentManager from here via bp_.sm_ as sm_ is private.
            }
            // fallback: allocate directly using a temporary segment manager via BufferPool - but our BufferPool doesn't expose it.
            // To keep integration simple, we will allocate by calling fetch_page on a page that is known not to exist.
            try {
                storage::PageId newpid{seg, page_no};
                // create page via segment manager directly: need access to SegmentManager. We don't have it here.
                // Instead, we will attempt to fetch and if it initializes blank page, we'll write.
                storage::Frame *frame = bp_.fetch_page(newpid, true);
                // write record
                uint32_t used = read_page_used_bytes(frame->page);
                uint32_t rec_len = static_cast<uint32_t>(payload.size());
                std::memcpy(frame->page.data + 4, &rec_len, 4);
                std::memcpy(frame->page.data + 8, payload.data(), payload.size());
                used += 4 + rec_len;
                write_page_used_bytes(frame->page, used);
                bp_.unpin_page(frame, true);
                written = true;
                break;
            } catch (const std::exception &e) {
                return std::string("ERR: failed to allocate/write page: ") + e.what();
            }
        } catch (const std::exception &e) {
            return std::string("ERR: I/O error during insert: ") + e.what();
        }
    }

    if (written) {
        return std::string("OK: 1 row inserted");
    } else {
        return std::string("ERR: failed to write row");
    }
}

std::string Executor::handle_select(const std::string &sql) {
    // support: SELECT * FROM <table>
    size_t pos_from = std::string::npos;
    // naive parse
    std::string upper = sql;
    std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c){ return std::toupper(c); });
    pos_from = upper.find("FROM");
    if (pos_from == std::string::npos) return "ERR: malformed SELECT";
    std::string tbl = trim(sql.substr(pos_from + 4));
    // remove trailing ; or where clause (not supported)
    auto wherepos = tbl.find("WHERE");
    if (wherepos != std::string::npos) tbl = trim(tbl.substr(0, wherepos));
    if (!tbl.empty() && tbl.back() == ';') tbl.pop_back();
    tbl = trim(tbl);

    auto maybe = catalog_.get_table(tbl);
    if (!maybe) return std::string("ERR: unknown table ") + tbl;
    const catalog::Table &table = *maybe;

    uint32_t seg = table_to_segment(tbl);
    std::ostringstream out;

    // scan pages 0.. until read_page throws
    uint32_t page_no = 0;
    while (true) {
        storage::PageId pid{seg, page_no};
        storage::Frame *frame = nullptr;
        try {
            frame = bp_.fetch_page(pid, false);
        } catch (const std::out_of_range &) {
            break;
        } catch (const std::exception &) {
            break;
        }
        // parse records in page
        uint32_t used = read_page_used_bytes(frame->page);
        uint32_t offset = 4;
        while (offset + 4 <= used + 4) {
            // read rec_len
            uint32_t rec_len = 0;
            std::memcpy(&rec_len, frame->page.data + offset, 4);
            if (rec_len == 0) break;
            offset += 4;
            if (offset + rec_len > sizeof(frame->page.data)) break;
            // parse payload
            size_t po = 0;
            std::vector<std::string> cols;
            while (po + 4 <= rec_len) {
                uint32_t col_len = 0;
                std::memcpy(&col_len, frame->page.data + offset + po, 4);
                po += 4;
                if (po + col_len > rec_len) break;
                std::string val(frame->page.data + offset + po, frame->page.data + offset + po + col_len);
                po += col_len;
                cols.push_back(val);
            }
            // print row
            for (size_t i = 0; i < cols.size() && i < table.columns.size(); ++i) {
                out << table.columns[i].name << "=" << cols[i];
                if (i + 1 < cols.size()) out << ", ";
            }
            out << "\n";

            offset += rec_len;
        }

        bp_.unpin_page(frame, false);
        page_no++;
    }

    std::string res = out.str();
    if (res.empty()) return "OK: 0 rows";
    return res;
}

std::string Executor::handle_create_table(const std::string &sql) {
    // parse table and columns (your existing code)
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
    if (nl == std::string::npos) return "ERR: missing table name";
    auto nr = name_part.find_last_not_of(" \t\r\n");
    std::string tblname = name_part.substr(nl, nr - nl + 1);

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
        if (cname.empty() || ctype.empty()) return "ERR: malformed column: " + cp;
        cols.push_back({cname, ctype});
    }

    std::string err;
    if (!catalog_.create_table(tblname, cols, err)) return "ERR: " + err;

    // --- Persist catalog immediately ---
    std::string path = "./data/catalog.meta";  // or use Engine's cfg_.data_dir
    if (!catalog_.save_to_file(path, err)) return "ERR: failed to save catalog: " + err;

    return "OK: table created: " + tblname;
}

std::string Executor::handle_update(const std::string &/*sql*/) {
    return "ERR: UPDATE not implemented in this version";
}

std::string Executor::handle_delete(const std::string &/*sql*/) {
    return "ERR: DELETE not implemented in this version";
}