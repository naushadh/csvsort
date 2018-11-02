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

  $ gsort --field-separator=',' -k3 data/comma-in-content.csv
  2,"123, Drink water drive",300 # <-- THIS SHOULD BE LAST!
  1,John Doe,100
  3,Bob Smith,200
  id,column1,column2 # <-- And this is annoying
  ```

## Performance

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

- Diff results

  ```bash
  $ diff -u .scratch/out/base.txt .scratch/out/x.txt
  # comparison
  ```

### Sample workload results

Stat|Value
---|---
OS|macOS High Sierra
CPU|2.3 GHz Intel Core i5
Memory|16 GB 2133 MHz LPDDR3

```diff
--- .scratch/out/base.txt       2018-11-01 22:15:13.000000000 -0400
+++ .scratch/out/x.txt  2018-11-01 22:22:37.000000000 -0400
@@ -1,21 +1,21 @@
-       Command being timed: "gsort --buffer-size=200M --key 1 /tmp/in.csv --output /tmp/out.csv"
-       User time (seconds): 128.72
-       System time (seconds): 0.72
-       Percent of CPU this job got: 301%
-       Elapsed (wall clock) time (h:mm:ss or m:ss): 0:42.88
+       Command being timed: "filesort --in /tmp/in.csv --keys [0] --out /tmp/out.csv"
+       User time (seconds): 75.17
+       System time (seconds): 12.43
+       Percent of CPU this job got: 240%
+       Elapsed (wall clock) time (h:mm:ss or m:ss): 0:36.47
        Average shared text size (kbytes): 0
        Average unshared data size (kbytes): 0
        Average stack size (kbytes): 0
        Average total size (kbytes): 0
-       Maximum resident set size (kbytes): 205752
+       Maximum resident set size (kbytes): 4126744
        Average resident set size (kbytes): 0
-       Major (requiring I/O) page faults: 1
-       Minor (reclaiming a frame) page faults: 102797
-       Voluntary context switches: 2
-       Involuntary context switches: 55134
+       Major (requiring I/O) page faults: 0
+       Minor (reclaiming a frame) page faults: 1031057
+       Voluntary context switches: 7
+       Involuntary context switches: 1609172
        Swaps: 0
        File system inputs: 0
-       File system outputs: 24
+       File system outputs: 17
        Socket messages sent: 0
        Socket messages received: 0
        Signals delivered: 0
```

## Getting started

### Build from source

- [Get stack](https://docs.haskellstack.org/en/stable/install_and_upgrade/)

- Build+Install

  ```bash
  $ build-perf
  # build output; installs to $HOME/.local/bin
  ```

## TODO

- [ ] Implement merging sorted chunks for arbitrary number of chunks.
- [ ] Add proper benchmarking suite with charts and various shapes/sizes of data
- [ ] Tune for performance.
