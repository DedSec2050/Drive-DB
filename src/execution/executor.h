#pragma once
#include "src/catalog/catalog.h"
#include "src/storage/buffer/buffer_pool.h"
#include <string>

class Executor {
public:
    Executor(catalog::Catalog &catalog, storage::BufferPool &bp);

    std::string execute(const std::string &sql);

private:
    catalog::Catalog &catalog_;
    storage::BufferPool &bp_;

    std::string handle_create_table(const std::string &sql);
    std::string handle_insert(const std::string &sql);
    std::string handle_select(const std::string &sql);
    std::string handle_update(const std::string &sql);
    std::string handle_delete(const std::string &sql);
};
