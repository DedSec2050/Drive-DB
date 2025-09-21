#include "src/cli/config.h"
#include <fstream>
#include <sstream>
#include <iostream>

std::optional<Config> Config::loadConfig(const std::string &path, std::string &err) {
    std::ifstream ifs(path);
    if (!ifs) {
        err = "could not open config file: " + path;
        return std::nullopt;
    }
    Config c;
    std::string line;
    while (std::getline(ifs, line)) {
        auto comment_pos = line.find('#');
        if (comment_pos != std::string::npos) line.erase(comment_pos);
        size_t a = line.find_first_not_of(" \t\r\n");
        if (a == std::string::npos) continue;
        size_t b = line.find_last_not_of(" \t\r\n");
        std::string token = line.substr(a, b - a + 1);
        auto eq = token.find('=');
        if (eq == std::string::npos) continue;
        std::string key = token.substr(0, eq);
        std::string val = token.substr(eq+1);
        auto trim = [](std::string &s){
        size_t a = s.find_first_not_of(" \t\r\n");
        if (a==std::string::npos) { s.clear(); return; }
            size_t b = s.find_last_not_of(" \t\r\n");
            s = s.substr(a, b-a+1);
        };
        trim(key); trim(val);
        if (key == "data_dir") c.data_dir = val;
        else if (key == "pid_file") c.pid_file = val;
        else if (key == "daemonize") c.daemonize = (val == "1" || val == "true" || val=="yes");
        else if (key == "ask_mode") c.ask_mode = (val == "1" || val == "true" || val=="yes");
        else c.extra[key] = val;
        }
        return c;
}