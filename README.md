# Spark Taxi Warehouse

A data warehouse built with Apache Spark (Java) over NYC yellow-taxi trip records: an ETL pipeline that cleans the raw files into a star schema in partitioned Parquet, OLAP queries (ROLLUP, a materialised CUBE, slice/dice, window ranking), an RDD implementation of the cube lattice checked against Spark SQL, and benchmarks that measure the storage and execution choices behind it.

## What it shows

| Area | Where |
| --- | --- |
| ETL with data-quality rules and a per-rule rejection report | `Etl.java` |
| Star schema: 1 fact table, 5 dimensions, partitioned by year/month | `Etl.java`, `Dimensions.java` |
| Spark SQL OLAP: `ROLLUP`, `CUBE`, `GROUPING()`, slice, dice, `RANK() OVER` | `Olap.java` |
| The cube lattice with RDDs (all 8 cuboids in one shuffle), verified cell by cell | `CubeLattice.java` |
| Benchmarks: CSV vs Parquet, partition pruning, broadcast vs sort-merge join, RDD vs DataFrame | `Benchmarks.java` |

## Schema

```
                    dim_date (date_key)
                          |
dim_time (hour_key) -- fact_trips -- dim_zone (zone_key: pickup + dropoff)
                          |
          dim_payment (payment_key)   dim_vendor (vendor_key)
```

`fact_trips`: date_key, hour_key, pu_zone_key, do_zone_key, payment_key, vendor_key, passenger_count, trip_distance, duration_min, fare_amount, tip_amount, total_amount, partitioned by year and month.

## Run it

Requirements: Java 17+, Maven 3.9+. Spark runs in local mode; no cluster or Spark install is needed.

```bash
chmod +x run.sh

# Option A: quick test with synthetic data (no download)
./run.sh sample --out data/sample --rows 2000000
./run.sh etl --raw data/sample/raw --zones data/sample/taxi_zone_lookup.csv --out out

# Option B: real data (see "Data" below)
./run.sh etl --raw data/raw --zones data/taxi_zone_lookup.csv --out out --from 2024-01 --to 2024-03

# Then
./run.sh olap --out out
./run.sh lattice --out out
./run.sh bench --out out
```

Results go to `out/`: the warehouse tables in `out/warehouse/`, query results as CSV in `out/olap/`, and `data_quality.csv` and `benchmarks.csv` in `out/reports/`. For more than about 3 months of data, raise driver memory: `SPARK_DRIVER_MEMORY=8g ./run.sh ...`.

## Data

From the [NYC TLC trip record data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page, download into `data/raw/`:

- Yellow Taxi Trip Records (Parquet) for the months you want, e.g. `yellow_tripdata_2024-01.parquet` to `2024-03`
- The Taxi Zone Lookup Table, saved as `data/taxi_zone_lookup.csv`

The data itself is not committed (see `.gitignore`).

## Results

Run on NYC yellow-taxi trips for January–March 2024 (9,554,778 raw trips), Spark 4.0.1 in local mode on a MacBook Pro.

### ETL

9,178,759 trips loaded in 9.6 s; 376,019 (3.94%) rejected. The biggest causes were zero or implausible distances (2.26%), negative or extreme amounts (1.43%), and trips under 1 minute or over 3 hours (1.21%). The fact table is 96 MB of Parquet.

### Benchmarks

| Experiment | Slower variant | Faster variant | Gain |
| --- | --- | --- | --- |
| Storage format (aggregate the whole fact table) | CSV: 740 ms, 526.6 MB | Parquet: 81 ms, 96.0 MB | **9.1× faster, 5.5× smaller** |
| Partition pruning (one-month report) | Unpartitioned: 101 ms, scans 115.3 MB | Partitioned by year/month: 74 ms, scans 30.2 MB | **1.4× faster, 69% less data read** |
| Join fact × dim_zone | Sort-merge join: 465 ms | Broadcast hash join: 127 ms | **3.7× faster** |
| Revenue per zone | RDD `groupByKey`: 909 ms | RDD `reduceByKey`: 171 ms / DataFrame `groupBy`: 66 ms | **5.3× / 13.8× faster** |

Each variant runs once to warm up, then 3 times; the table shows the median. Every run builds a fresh query, because re-running one DataFrame object lets Spark reuse its earlier shuffle files. The join variant Spark actually used is read from the physical plan, and the partitioned query's plan is checked for a partition filter.

Why the gaps:
- **Parquet** stores each column separately and compressed, so an aggregate reads only the columns it uses.
- **Partitioning** puts each month in its own folder, so a one-month query skips the other two.
- **A broadcast join** sends the 265-row zone table to every task instead of shuffling 9 million fact rows.
- **`reduceByKey`** sums on each node before the shuffle, while `groupByKey` ships every value. The DataFrame version adds Catalyst's optimised plan and a columnar scan on top.

### What the data says

| Question | Answer |
| --- | --- |
| Top revenue pickup zone | JFK Airport: $33.2M from 404,298 trips, 3.1× Midtown Center ($10.7M from 441,043 trips) |
| Busiest Manhattan time band | Midday (10:00–15:59): 2.69M trips, $61.8M |
| Card tip rate by hour | Lowest 18.6% of fare at 5 am, highest 24.2% at 6 pm |
| Weekday vs weekend speed | 12.4 mph on weekdays, 13.7 mph at weekends |
| Revenue, Q1 2024 | $252.1M; March highest at $95.4M |

The RDD lattice and Spark SQL's `CUBE` agree on all 306 cells (8 cuboids).

## Cleaning rules

A trip is rejected if any rule fails. Each rule is counted separately in `reports/data_quality.csv`.

| Rule | Rejects |
| --- | --- |
| bad_time | missing timestamps, or pickup outside `--from`..`--to` |
| bad_duration | trips shorter than 1 minute or longer than 180 minutes |
| bad_distance | distance of 0 or less, or over 100 miles |
| bad_amount | negative fare or total, or total over $1,000 |
| bad_zone | pickup or dropoff zone missing or outside 1–265 |
