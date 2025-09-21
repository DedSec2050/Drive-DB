#pragma once


#include <string>
#include <optional>
#include <unordered_map>


struct Config {
std::string data_dir = "./data";
std::string pid_file = "./boltd.pid";
bool daemonize = false;
bool ask_mode = true; // prompt user if true
std::unordered_map<std::string,std::string> extra;


static std::optional<Config> loadConfig(const std::string &path, std::string &err);
};