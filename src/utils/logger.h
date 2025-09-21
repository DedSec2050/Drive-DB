#pragma once


#include <string>
#include <iostream>


enum class LogLevel { DEBUG, INFO, WARN, ERROR };


inline void log(LogLevel lvl, const std::string &msg) {
const char *tag = "INFO";
switch (lvl) { case LogLevel::DEBUG: tag = "DEBUG"; break; case LogLevel::WARN: tag = "WARN"; break; case LogLevel::ERROR: tag = "ERROR"; break; default: break; }
std::cerr << "[" << tag << "] " << msg << std::endl;
}