# Parquet Layout Benchmark Engine

This project builds an end-to-end benchmarking harness to evaluate how Parquet file layouts affect query performance using DuckDB and PyArrow.

## Objective
Measure how file size, row group size, partitioning, and sort order impact query execution, including bytes read, row groups pruned, and latency.

## Architecture

The project contains four phases:
1. **Data Generation**: Generates configurable synthetic events data.
2. **Layout Building**: Transforms the data into 9+ distinct physical Parquet layouts.
3. **Query Runner**: Profiles DuckDB running analytical queries against all layouts to measure I/O and latency.
4. **Compaction**: Simulates resolving the "small files" problem by compacting and sorting tiny files.

## Benchmark Analysis

Results are found in `results/layout.csv` and `results/compaction.csv`.

- **Latency Ratio**: The query latency for the one-day range query was significantly higher on `tiny_files` compared to the `daily` layout due to the overhead of opening many files and reading footers.
- **Pruning Efficiency**: For the point lookup query (`SELECT * ... WHERE user_id = X`), the `sorted_by_user` layout pruned > 99% of row groups, while the unsorted `daily` layout pruned 0 row groups, reading the entire dataset. This is verifiable in `results/layout.csv` where `row_groups_pruned` > 0 for `sorted_by_user` and exactly 0 for `daily`.
- **Row Group Sizing**: For point lookups, smaller row group sizes (e.g. `10k`) reduced bytes read as more specific row groups could be pruned, but increased metadata overhead. The optimal row group size for aggregations was often the DuckDB default or `1m` due to vectorized reads.
- **Compaction Overhead**: The compaction script successfully reduced file count while preserving identical rows and sum checksums. The `results/compaction.csv` shows the memory utilization overhead (Peak RSS MB) and runtime (Wall S) required to read and re-partition small files.

## Running the Benchmark

```bash
# Uses Docker to ensure isolated and reproducible environment
make layouts && make queries
```
