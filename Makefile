define USAGE

Available commands:

	- build: Compile all targets in pedantic mode.

	- help: Print this usage prompt.

endef
export USAGE

help:
	@echo "$$USAGE"
.PHONY: help

build:
	stack build --pedantic
.PHONY: build

build-perf:
	stack build --pedantic --ghc-options "-O2 -optc-O3 -optc-ffast-math -fforce-recomp"
	stack install filesort
.PHONY: build-perf

seed-1M: build
	stack exec -- filesort-gen --out /tmp/in.csv --limit 1000000
	du -h /tmp/in.csv
.PHONY: seed-1M

seed-10M: build
	stack exec -- filesort-gen --out /tmp/in.csv --limit 10000000
	du -h /tmp/in.csv
.PHONY: seed-10M

run-base:
	(gtime -v gsort --buffer-size=200M --key 1 /tmp/in.csv --output /tmp/out.csv) &> .scratch/out/base.txt
.PHONY: run-base

run-x:
	(gtime -v filesort --in /tmp/in.csv --keys "[0]" --out /tmp/out.csv) &> .scratch/out/x.txt
.PHONY: run-x
