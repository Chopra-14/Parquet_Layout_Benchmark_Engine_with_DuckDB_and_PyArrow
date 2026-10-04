.PHONY: all clean layouts queries bench test

# Default rows to 1,000,000 for the automated test runner
ROWS ?= 1000000

all: layouts queries

layouts:
	mkdir -p data/raw data/layouts results
	python src/generate_data.py --rows $(ROWS) --out_dir data/raw
	python src/build_layouts.py --input data/raw/events.parquet --out_dir data/layouts

queries:
	mkdir -p results
	python src/run_queries.py --input data/layouts --out results/layout.csv
	python src/compact.py --input data/layouts/tiny_files --out data/layouts/compacted --stats results/compaction.csv

clean:
	rm -rf data/raw/* data/layouts/* results/*
