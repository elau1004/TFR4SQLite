# ========================================================
# Makefile: Explicit compilation of shell.c, glob.c, tfr.c
# ========================================================

# ----------------------------
# Build type (debug/release)
# ----------------------------
BUILD_TYPE ?= release

# ----------------------------
# Compiler and base flags
# ----------------------------
CC      = gcc
CFLAGS  = -Wall -Iinc

ifeq ($(BUILD_TYPE),debug)
    CFLAGS += -g
else
    CFLAGS += -O2
endif

# ----------------------------
# Directories
# ----------------------------
SRC_DIR     = src
LIB_DIR     = $(SRC_DIR)/lib
MAIN_DIR    = $(SRC_DIR)/main
TEST_DIR    = $(SRC_DIR)/test
BUILD_ROOT  = build
BUILD_DIR   = $(BUILD_ROOT)/$(BUILD_TYPE)

# ----------------------------
# OS detection
# ----------------------------
ifeq ($(OS),Windows_NT)
    LIB_EXT      = .dll
    EXE_EXT      = .exe
    SHARED_FLAGS = -shared
    RM           = rm -rf
    MKDIR_P      = mkdir -p
else
    LIB_EXT      = .so
    EXE_EXT      =
    SHARED_FLAGS = -fPIC -shared
    RM           = rm -rf
    MKDIR_P      = mkdir -p
endif

# ----------------------------
# Output targets
# ----------------------------
LIB_NAME   = $(BUILD_DIR)/tfr$(LIB_EXT)
MAIN_BIN   = $(BUILD_DIR)/shell$(EXE_EXT)

# ----------------------------
# Unit test sources
# ----------------------------
TEST_SRCS  := $(wildcard $(TEST_DIR)/*.c)
TEST_OBJS  := $(patsubst $(TEST_DIR)/%.c,$(BUILD_DIR)/%.o,$(TEST_SRCS))
TEST_BINS  := $(patsubst $(TEST_DIR)/%.c,$(BUILD_DIR)/%$(EXE_EXT),$(TEST_SRCS))

# ----------------------------
# Default target
# ----------------------------
all: $(LIB_NAME) $(MAIN_BIN) $(TEST_BINS)
	@echo "Build type: $(BUILD_TYPE)"
	@echo "All targets built in $(BUILD_DIR)"

# ========================================================
# Compile glob.c (library helper)
# ========================================================
$(BUILD_DIR)/glob.o: $(LIB_DIR)/glob.c
	@$(MKDIR_P) $(BUILD_DIR)
	# $@ = build/debug/glob.o
	# $< = src/lib/glob.c
	$(CC) $(CFLAGS) -c $< -o $@
	@echo "Compiled: $< → $@"

# ========================================================
# Compile tfr.c (depends on glob.c)
# ========================================================
$(BUILD_DIR)/tfr.o: $(LIB_DIR)/tfr.c $(BUILD_DIR)/glob.o
	@$(MKDIR_P) $(BUILD_DIR)
	# Include glob.o dependency
	$(CC) $(CFLAGS) -c $< -o $@
	@echo "Compiled: $< → $@"

# ========================================================
# Build shared library (tfr.dll / tfr.so)
# ========================================================
$(LIB_NAME): $(BUILD_DIR)/glob.o $(BUILD_DIR)/tfr.o
	@$(MKDIR_P) $(BUILD_DIR)
	# $@ = build/debug/tfr.dll or tfr.so
	# $^ = glob.o tfr.o
	$(CC) $(SHARED_FLAGS) -o $@ $^
	@echo "Built shared library: $@"

# ========================================================
# Compile shell.c (main program)
# ========================================================
$(BUILD_DIR)/shell.o: $(MAIN_DIR)/shell.c
	@$(MKDIR_P) $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@
	@echo "Compiled: $< → $@"

# ========================================================
# Link shell executable
# ========================================================
$(MAIN_BIN): $(BUILD_DIR)/shell.o $(LIB_NAME)
	@$(MKDIR_P) $(BUILD_DIR)
ifeq ($(OS),Windows_NT)
	# Windows: link directly with DLL
	$(CC) $^ -o $@
else
	# Linux: link with shared library
	$(CC) $^ -L$(BUILD_DIR) -ltfr -o $@
endif
	@echo "Built main program: $@"

# ========================================================
# Unit test compilation
# ========================================================
$(BUILD_DIR)/%.o: $(TEST_DIR)/%.c
	@$(MKDIR_P) $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@
	@echo "Compiled test: $< → $@"

$(BUILD_DIR)/%$(EXE_EXT): $(BUILD_DIR)/%.o $(LIB_NAME)
ifeq ($(OS),Windows_NT)
	$(CC) $^ -o $@
else
	$(CC) $^ -L$(BUILD_DIR) -ltfr -o $@
endif
	@echo "Built test executable: $@"

# ========================================================
# Clean all artifacts
# ========================================================
clean:
	$(RM) $(BUILD_ROOT)

# ========================================================
# Phony targets
# ========================================================
.PHONY: all clean
