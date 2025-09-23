cmake_minimum_required(VERSION 4.1.1)
project(DriveDB VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add source directories
set(SRC_MAIN
    src/main/launcher.cpp
    src/main/repl_launcher.cpp
    src/cli/config.cpp
    src/main/daemon_launcher.cpp
    src/engine/engine.cpp
    src/catalog/catalog.cpp
    src/storage/segment/segment_manager.cpp   
    src/storage/buffer/buffer_pool.cpp
    src/execution/executor.cpp 
)

# Include directories
include_directories(${CMAKE_SOURCE_DIR})


# Create executable
add_executable(boltd ${SRC_MAIN})

# Optional: link pthread for signal handling (daemon)
find_package(Threads REQUIRED)
target_link_libraries(boltd Threads::Threads)
