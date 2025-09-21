#include "src/cli/config.h"
#include "src/main/repl_launcher.h"
#include "src/utils/logger.h"


#include <getopt.h>
#include <iostream>


static void print_usage(const char *prog) {
std::cout << "Usage: " << prog << " [--config PATH] [--mode repl|daemon] [--yes]" << std::endl;
}


int main(int argc, char **argv) {
std::string config_path;
std::string mode;
bool yes = false;


const struct option longopts[] = {
{"config", required_argument, nullptr, 'c'},
{"mode", required_argument, nullptr, 'm'},
{"yes", no_argument, nullptr, 'y'},
{"help", no_argument, nullptr, 'h'},
{nullptr, 0, nullptr, 0}
};


int opt;
while ((opt = getopt_long(argc, argv, "c:m:y h", longopts, nullptr)) != -1) {
switch (opt) {
case 'c': config_path = optarg; break;
case 'm': mode = optarg; break;
case 'y': yes = true; break;
case 'h': print_usage(argv[0]); return 0;
default: print_usage(argv[0]); return 1;
}
}


Config cfg;
if (!config_path.empty()) {
std::string err;
auto loaded = Config::loadConfig(config_path, err);
if (!loaded) {
log(LogLevel::WARN, "Failed to load config: " + err + " - using defaults");
} else cfg = *loaded;
}


if (yes) cfg.ask_mode = false;
if (!mode.empty()) {
if (mode == "daemon") cfg.daemonize = true;
else if (mode == "repl") cfg.daemonize = false;
}


if (cfg.ask_mode && mode.empty()) {
std::cout << "Run in (1) REPL or (2) Daemon? [1/2]: " << std::flush;
std::string answer;
if (!std::getline(std::cin, answer)) return 1;
if (answer == "2") cfg.daemonize = true; else cfg.daemonize = false;
}


if (cfg.daemonize) {
log(LogLevel::INFO, "Selected daemon mode");
return start_daemon(cfg);
} else {
log(LogLevel::INFO, "Selected REPL mode");
return start_repl(cfg);
}
}