# =========================================================
# Cross-Platform Makefile: Linux GCC / MinGW-w64
# =========================================================
# Project layout:
# inc/                 → public headers
# src/lib/             → reusable library (tfr.c, glob.c)
# src/main/            → main program (shell.c, sqlite3.c)
# build/{debug,release}/
# =========================================================
# Usage: make BUILD_TYPE=[debug|release]
BUILD_TYPE ?= release

# ----------------------------
# Compiler and base flags
# ----------------------------
CC      := gcc
CFLAGS  := -Wall -Iinc
LDFLAGS := 

ifeq ($(BUILD_TYPE),debug)
    CFLAGS += -g -DDEBUG
else
    CFLAGS += -O2
endif

# ----------------------------
# Directories
# ----------------------------
SRC_DIR   	:= src
LIB_DIR     := $(SRC_DIR)/lib
MAIN_DIR    := $(SRC_DIR)/main
TEST_DIR    := $(SRC_DIR)/test
BUILD_ROOT  := build
BUILD_DIR   := $(BUILD_ROOT)/$(BUILD_TYPE)

# ----------------------------
# OS detection
# ----------------------------
ifeq ($(OS),Windows_NT)
    LIB_EXT      	:= .dll
    EXE_EXT      	:= .exe
    SHARED_FLAGS 	:= -shared
    RM           	:= rm -rf
    MKDIR_P      	:= mkdir -p
    SHELL_LDFLAGS	:=  # Windows: dl/m math not needed
else
    LIB_EXT      	:= .so
    EXE_EXT      	:=
    SHARED_FLAGS 	:= -fPIC -shared
    RM           	:= rm -rf
    MKDIR_P      	:= mkdir -p
    SHELL_LDFLAGS 	:= -ldl -lm
endif

# ----------------------------
# Output targets
# ----------------------------
LIB_NAME   := $(BUILD_DIR)/tfr$(LIB_EXT)
MAIN_BIN   := $(BUILD_DIR)/shell$(EXE_EXT)

# ----------------------------
# Source files
# ----------------------------
GLOB_SRC   := $(LIB_DIR)/glob.c
TFR_SRC    := $(LIB_DIR)/tfr.c
SHELL_SRC  := $(MAIN_DIR)/shell.c
SQLITE_SRC := $(MAIN_DIR)/sqlite3.c

# ----------------------------
# Object files
# ----------------------------
GLOB_OBJ   := $(BUILD_DIR)/glob.o
TFR_OBJ    := $(BUILD_DIR)/tfr.o
SHELL_OBJ  := $(BUILD_DIR)/shell.o
SQLITE_OBJ := $(BUILD_DIR)/sqlite3.o

# ----------------------------
# Default target
# ----------------------------
all: $(BUILD_DIR) $(LIB_NAME) $(MAIN_BIN)

# ========================================================
# Create build directory
# ========================================================
$(BUILD_DIR):
	@$(MKDIR_P) $@

# ========================================================
# Compile glob.c (only on Windows)
# ========================================================
ifeq ($(OS),Windows_NT)
$(GLOB_OBJ): $(GLOB_SRC) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@
	@echo "Compiled: $< → $@"
endif

# ========================================================
# Compile tfr.c as shared library
# ========================================================
$(TFR_OBJ): $(TFR_SRC) $(GLOB_OBJ)
	$(CC) $(CFLAGS) -c $< -o $@
	@echo "Compiled: $< → $@"

$(LIB_NAME): $(TFR_OBJ)
ifeq ($(OS),Windows_NT)
	$(CC) $(SHARED_FLAGS) -o $@ $(TFR_OBJ) $(GLOB_OBJ)
else
	$(CC) $(SHARED_FLAGS) -o $@ $(TFR_OBJ)
endif
	@echo "Built shared library: $@"

# ========================================================
# Compile shell.c and sqlite3.c
# ========================================================
$(SQLITE_OBJ): $(SQLITE_SRC) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -Wno-unused-variable -DSQLITE_THREADSAFE=0 -c $< -o $@
	@echo "Compiled: $< → $@"

$(SHELL_OBJ): $(SHELL_SRC) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -Wno-unused-variable -DSQLITE_THREADSAFE=0 -c $< -o $@
	@echo "Compiled: $< → $@"

# ========================================================
# Link shell executable
# ========================================================
$(MAIN_BIN): $(SHELL_OBJ) $(SQLITE_OBJ)
	$(CC) $^ $(SHELL_LDFLAGS) -o $@
	@echo "Built main program: $@"

# ========================================================
# Clean
# ========================================================
clean:
	$(RM) $(BUILD_ROOT)

.PHONY: all clean
