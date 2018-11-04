# `filesort`

A pure haskell alternative to the [GNU `sort`](http://man7.org/linux/man-pages/man1/sort.1.html).

**NOTE**: Project is still a work in progress! See [TODO](#TODO).

## Motivation

- Surprised that there already wasn't a native haskell function/library to sort file/data larger than memory.
  - Invoking a system command is a bit clunky; at minimum not all OSes have the same command/API (ex: MacOS does not support parallel execution).
  - Haskell can effortlessly use [_all the cores_ and more](https://simonmar.github.io/posts/2016-12-08-Haskell-in-the-datacentre.html).

- GNU `sort` is not [RFC 4180](https://tools.ietf.org/html/rfc4180) compliant in how it handles separators.

  ```bash
  $ cat data/comma-in-content.csv
  id,column1,column2
  3,Bob Smith,200
  2,"123, Drink water drive",300
  1,John Doe,100

  $ sort --field-separator=',' -k3 data/comma-in-content.csv
  2,"123, Drink water drive",300 # <-- THIS SHOULD BE LAST!
  1,John Doe,100
  3,Bob Smith,200
  id,column1,column2 # <-- And this is annoying
  ```

## Getting started

### Build from source

- [Get stack](https://docs.haskellstack.org/en/stable/install_and_upgrade/)

- Build+Install

  ```bash
  $ build-perf
  # build output; installs to $HOME/.local/bin
  ```

## Performance

### Setup

- MacOS

  ```bash
  $ brew install gnu-time coreutils
  $ ln -s /usr/local/bin/gtime /usr/local/bin/time
  $ ln -s /usr/local/bin/sort /usr/local/bin/sort
  # this should bring GNU sort and time into $PATH
  ```

- Windows 10: best to try and use [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)

### Sample workload

- Create a 10M row file with reverse sorted integers.

  ```bash
  $ make seed-10M
  # creates file at /tmp/in.csv
  ```

- Run baseline sort

  ```bash
  $ make run-base
  # stores runtime stats in .scratch/out/base.txt
  ```

- Run new sort

  ```bash
  $ make run-x
  # stores runtime stats in .scratch/out/x.txt
  ```

- Compare

  ```bash
  $ diff -u .scratch/out/base.txt .scratch/out/x.txt
  # comparison
  ```

- Results

  Stat|Value
  ---|---
  OS|macOS High Sierra
  CPU|2.3 GHz Intel Core i5
  Memory|16 GB 2133 MHz LPDDR3

  ```diff
  -       User time (seconds): 128.78
  -       System time (seconds): 0.59
  -       Percent of CPU this job got: 301%
  -       Elapsed (wall clock) time (h:mm:ss or m:ss): 0:42.98
  +       User time (seconds): 130.26
  +       System time (seconds): 27.13
  +       Percent of CPU this job got: 249%
  +       Elapsed (wall clock) time (h:mm:ss or m:ss): 1:03.12
          Average shared text size (kbytes): 0
          Average unshared data size (kbytes): 0
          Average stack size (kbytes): 0
          Average total size (kbytes): 0
  -       Maximum resident set size (kbytes): 205736
  +       Maximum resident set size (kbytes): 4715024
          Average resident set size (kbytes): 0
  -       Major (requiring I/O) page faults: 1
  -       Minor (reclaiming a frame) page faults: 102795
  -       Voluntary context switches: 6
  -       Involuntary context switches: 37514
  +       Major (requiring I/O) page faults: 0
  +       Minor (reclaiming a frame) page faults: 1178750
  +       Voluntary context switches: 12
  +       Involuntary context switches: 8021520
          Swaps: 0
  -       File system inputs: 0
  -       File system outputs: 21
  +       File system inputs: 1
  +       File system outputs: 16
          Socket messages sent: 0
          Socket messages received: 0
          Signals delivered: 0
  ```

## TODO

- [ ] Add proper benchmarking suite with charts and various shapes/sizes of data
- [ ] Tune for performance.
