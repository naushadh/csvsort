# `filesort`

A pure haskell (re)implementation of the [GNU `sort`](http://man7.org/linux/man-pages/man1/sort.1.html).

## Motivation

- Surprised that there already wasn't a native haskell function/library to sort file/data larger than memory.
  - Invoking a system command is a bit clunky; at minimum not all OSes have the same command/API (ex: MacOS does not support parallel execution).
  - Haskell can painlessly use [_all the cores_ and more](https://simonmar.github.io/posts/2016-12-08-Haskell-in-the-datacentre.html).

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
