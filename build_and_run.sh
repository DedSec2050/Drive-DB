#!/usr/bin/env bash
set -e

# --- Colors ---
RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
BLUE="\033[1;34m"
NC="\033[0m"

# --- Directories ---
BUILD_DIR=build/mac
BIN_DIR=bin/mac
DATA_DIR=data
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Ask for approval to start ---
echo -e "${YELLOW}This script will build the BoltDB CLI/REPL for macOS.${NC}"
read -p "Do you want to continue? (y/N) " approval
if [[ ! "$approval" =~ ^[Yy]$ ]]; then
    echo -e "${RED}Aborting.${NC}"
    exit 0
fi

# --- Ask for cleanup ---
if [ -d "$BUILD_DIR" ]; then
    read -p "Old build directory exists. Do you want to remove it? (y/N) " clean_approval
    if [[ "$clean_approval" =~ ^[Yy]$ ]]; then
        echo -e "${BLUE}Cleaning old build directory...${NC}"
        rm -rf "$BUILD_DIR"
        echo -e "${BLUE}Cleaning old data directory...${NC}"
        rm -rf "$DATA_DIR"
    fi
fi

mkdir -p "$BUILD_DIR" "$BIN_DIR"

# --- Step 1: Configure ---
echo -e "${BLUE}Step 1: Configuring CMake...${NC}"
pushd "$BUILD_DIR" >/dev/null
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_ARCHITECTURES=x86_64 "$PROJECT_ROOT"

# --- Step 2: Build ---
echo -e "${BLUE}Step 2: Building project...${NC}"
cmake --build . -j$(sysctl -n hw.ncpu)
popd >/dev/null

# --- Step 3: Copy binary ---
echo -e "${BLUE}Step 3: Copying binary to ${BIN_DIR}...${NC}"
cp "$BUILD_DIR/boltd" "$BIN_DIR/"

# --- Step 4: Completion ---
echo -e "${GREEN}Build completed successfully!${NC}"
echo -e "Binary location: ${BIN_DIR}/boltd"

# --- Step 5: Run approval ---
read -p "Do you want to run the binary now? (y/N) " run_approval
if [[ "$run_approval" =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}Running BoltDB CLI...${NC}"
    "$BIN_DIR/boltd"
else
    echo -e "${YELLOW}Exiting without running.${NC}"
fi
