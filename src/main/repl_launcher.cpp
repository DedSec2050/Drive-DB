#include "src/main/repl_launcher.h"
#include "src/utils/logger.h"


#include <csignal>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>


static volatile std::sig_atomic_t g_terminate = 0;


static void signal_handler(int sig) {
if (sig == SIGTERM || sig == SIGINT) g_terminate = 1;
}


int start_repl(Config &cfg) {
log(LogLevel::INFO, "Starting REPL mode (foreground)");
std::signal(SIGINT, signal_handler);
std::signal(SIGTERM, signal_handler);


std::string line;
while (!g_terminate) {
std::cout << "boltd> " << std::flush;
if (!std::getline(std::cin, line)) break;
if (line == ":quit" || line == ":exit") break;
if (line == ":help") {
std::cout << "Commands: :help :quit :backup :stats" << std::endl;
continue;
}
if (line.empty()) continue;
std::cout << "(not implemented) received: " << line << std::endl;
}
log(LogLevel::INFO, "REPL exiting");
return 0;
}

static bool write_pidfile(const std::string &path, pid_t pid, std::string &err) {
std::ofstream ofs(path, std::ofstream::trunc);
if (!ofs) { err = "failed to open pidfile for writing: " + path; return false; }
ofs << pid << "\n";
if (!ofs) { err = "failed to write pidfile"; return false; }
return true;
}


int start_daemon(Config &cfg) {
log(LogLevel::INFO, "Starting daemon mode");


pid_t pid = fork();
if (pid < 0) {
log(LogLevel::ERROR, "fork failed");
return 1;
}
if (pid > 0) {
log(LogLevel::INFO, "daemon forked, parent exiting");
return 0;
}
if (setsid() < 0) {
log(LogLevel::ERROR, "setsid failed");
return 1;
}
pid = fork();
if (pid < 0) { log(LogLevel::ERROR, "second fork failed"); return 1; }
if (pid > 0) {
_exit(0);
}


umask(027);
if (chdir("/") != 0) {
log(LogLevel::WARN, "chdir to / failed");
}


int fd = open("/dev/null", O_RDWR);
if (fd >= 0) {
dup2(fd, STDIN_FILENO);
dup2(fd, STDOUT_FILENO);
dup2(fd, STDERR_FILENO);
if (fd > 2) close(fd);
}


std::string err;
pid_t mypid = getpid();
if (!write_pidfile(cfg.pid_file, mypid, err)) {
log(LogLevel::ERROR, err);
return 1;
}
log(LogLevel::INFO, std::string("daemon running pid=") + std::to_string(mypid));


std::signal(SIGTERM, signal_handler);
std::signal(SIGINT, signal_handler);
std::signal(SIGHUP, SIG_IGN);


while (!g_terminate) {
sleep(1);
}


log(LogLevel::INFO, "daemon shutting down");
unlink(cfg.pid_file.c_str());
return 0;
}