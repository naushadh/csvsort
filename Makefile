define USAGE

Available commands:

	- build: Compile all targets in pedantic mode with performance flags turned on.

	- seed: Seed a data file with 1 Million rows (customize using ROWS).

	- run-base: Run `sort` and capture runtime stats.

	- run-x: Run `filesort` and capture runtime stats.

	- test: A/B Test ./data file(s).

	- clean: Purge the debug working dir.

	- help: Print this usage prompt.

endef
export USAGE

TIMESTAMP := $(shell date +%Y-%m-%dT%H:%M:%S)
MARKER ?= default
ROWS ?= 1M

help:
	@echo "$$USAGE"
.PHONY: help

build:
	stack build --pedantic --ghc-options "-O2 -optc-O3 -optc-ffast-math -fforce-recomp"
	stack install filesort
.PHONY: build

seed: build
	stack exec -- filesort-gen --out /tmp/in-$(ROWS).csv --limit $(ROWS)
	du -h /tmp/in-$(ROWS).csv
.PHONY: seed

run-base:
	(/usr/local/bin/time -v sort --key 1 /tmp/in-$(ROWS).csv --output /tmp/out-base-$(ROWS).csv) \
	&> .scratch/out/base-$(ROWS)-$(TIMESTAMP)-$(MARKER).txt
.PHONY: run-base

run-x: build clean
	(/usr/local/bin/time -v filesort --keys "[0]" --in /tmp/in-$(ROWS).csv --output /tmp/out-x-$(ROWS).csv) \
	&> .scratch/out/x-$(ROWS)-$(TIMESTAMP)-$(MARKER).txt
.PHONY: run-x

test: build clean
	sort --key 1,1 --output /tmp/test-base.csv data/comma-in-content.csv
	filesort --keys="[0]" --output /tmp/test-x.csv --in data/comma-in-content.csv
	diff -u /tmp/test-base.csv /tmp/test-x.csv
.PHONY: test

clean:
	rm .scratch/debug/*
.PHONY: clean
