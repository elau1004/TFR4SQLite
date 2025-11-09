# Contributing

## Setting up your environment

### Windows
You need to download and install the following software for it is **not** by default installed in a Windows environment.

| Software | File Version                 | Download page                          | Comment                        |
|----------|------------------------------|----------------------------------------|--------------------------------|
| VS Code  | >= VSCodeSetup-x64-1.105.1   | https://code.visualstudio.com/Download | Pick **System Installer** x86. |
| Git      | >= Git-2.51.2-64-bit.exe     | https://git-scm.com/install/windows    | Pick **Git for Windows/x64** Setup. |
| Mingw-64 | >= msys2-x86_64-20250830.exe | https://www.msys2.org/                 | gcc v15.2.0 |


* https://code.visualstudio.com/docs/cpp/config-mingw
* https://git-scm.com/install/windows
* https://www.msys2.org/

### Installation
Launch the above installations using **Administrator** privilege.  Install the above software with the following specifics:

<details open><summary><strong>Git</strong>:</summary>
We recommend that you install Git under `C:\Program Files\Git`
</details>
<br>
<details open><summary><strong>MSys2 / MinGW-64</strong>:</summary>
We recommend that you install Git under `C:Program Files\MSys64`.  Once you have install `MSys2`, launch the `MSys2` shell to install the development tool chain.

Run the following:
```bash
    pacman -S --needed base-devel   mingw-w64-ucrt-x86_64-toolchain  mingw-w64-x86_64-check
    pacman -Syu
```
When prompted accept the default to install everything.

Many of the binaries provided by `MSys2` overlap with those provided by `Git`.  You should prioritize those provided by `Git` over those provided  by `Msys2` because it is easier to upgrade `Git`.

Next you need to add the following value: 
1. `C:\Program Files\MSys64\ucrt64\bin` 
2. `C:\Program Files\MSys64\usr\bin` 

to your `PATH` environment variable.

Open a new command terminal and check your installation:
```bash
    gcc --version   # C compiler
    g++ --version   # C++ compiler
    gdb --version   # Debugger
```

<!--
We need `glob.h`, which is POSIX, however MinGW is **not** POSIX compliant.  We need to manually copy these files:
1. https://codebrowser.dev/glibc/glibc/posix/glob.c.html
2. https://codebrowser.dev/glibc/glibc/posix/glob.h.html
-->

Reference: https://code.visualstudio.com/docs/cpp/config-mingw#_prerequisites
</details>
<br>

<details open><summary><strong>VS Code</strong>:</summary>
Reference: https://code.visualstudio.com/docs/cpp/config-mingw#_prerequisites
</details>
<br>

<details open><summary><strong>Git Repository</strong>:</summary>

Clone the [Text File Reader for SQLite](https://github.com/elau1004/TFR4SQLite) with the following command:
```bash
    git clone   https://github.com/elau1004/TFR4SQLite.git
    cd  TFR4SQLite
```
</details>
<br>

<details open><summary><strong>SQLite source code</strong>:</summary>

Download the **amalgamated** version of SQLite that you want work with from [SQLite Download](https://sqlite.org/download.html) into this project repo root directory.
Unzip the files in amalgamated zip file into their respective sub-directories:
```bash
    unzip -l sqlite-amalgamation-3510000.zip
    unzip -p sqlite-amalgamation-3510000.zip  sqlite-amalgamation-3510000/sqlite3.h     > inc/sqlite3.h
    unzip -p sqlite-amalgamation-3510000.zip  sqlite-amalgamation-3510000/sqlite3ext.h  > inc/sqlite3ext.h
    unzip -p sqlite-amalgamation-3510000.zip  sqlite-amalgamation-3510000/sqlite3.c     > src/lib/sqlite3.c
    unzip -p sqlite-amalgamation-3510000.zip  sqlite-amalgamation-3510000/shell.h       > src/main/sqlite3.h
```
or copy them through GUI file manager.

Reference: https://sqlite.org/howtocompile.html#compiling_the_command_line_interface
</details>
<br>

<details open><summary><strong>Build the software</strong>:</summary>

Build the SQLite `shell` and the `trf` shared library with the following command:
```bash
    # Clean out old artifacts.
    make    clean
    
    # Build the software.
    make
    
    # Enable debugging (optional).
    make    BUILD_TYPE=debug

    ls -rl  shell  *.dll  *.exe  *.so 
```
The compiled artifacts shall be deposited into either `build\release` or `build\debug` sub-directory depending on `make` option.
Copy the `shell` executable and `tfr` shared library to where ever you like.
</details>
<br>

<details open><summary><strong>Run the SQLite shell</strong>:</summary>

Type in the following command to invoke the compiled SQLite `shell`:
```bash
    ./build/release/shell
```
</details>
<br>
