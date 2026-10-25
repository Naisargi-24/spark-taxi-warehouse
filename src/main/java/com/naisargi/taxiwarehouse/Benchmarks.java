package com.naisargi.taxiwarehouse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.spark.sql.functions.*;

/**
 * Four experiments, each answering a question from the course with numbers:
 *   1. Storage format: the same aggregation over CSV vs Parquet (time and size on disk).
 *   2. Partition pruning: one month's query on the partitioned fact vs an unpartitioned copy.
 *   3. Join strategy: broadcast hash join vs sort-merge join (fact x dim_zone).
 *   4. Aggregation API: RDD groupByKey vs RDD reduceByKey vs DataFrame groupBy.
 * Each variant runs --runs times (default 3); the median is reported. Results: reports/benchmarks.csv.
 */
public final class Benchmarks {
    private Benchmarks() { }

    private record Result(String benchmark, String variant, double medianMs, long rows, String note) { }

    public static void run(SparkSession spark, Args args) throws IOException {
        String out = args.get("out", "out");
        int runs = args.getInt("runs", 3);
        List<Result> results = new ArrayList<>();

        Dataset<Row> fact = spark.read().parquet(Locations.warehouse(out, "fact_trips"));
        Dataset<Row> zones = spark.read().parquet(Locations.warehouse(out, "dim_zone"));

        // Copies used for comparison (written once per run of this command).
        String csvDir = Locations.bench(out, "fact_csv");
        String flatDir = Locations.bench(out, "fact_flat_parquet");
        fact.write().mode(SaveMode.Overwrite).option("header", "true").csv(csvDir);
        // Round-robin repartition mixes months in every file, like data dumped with no layout.
        // (Written in month order instead, Parquet's row-group min/max stats would skip most of it anyway.)
        fact.repartition(8).write().mode(SaveMode.Overwrite).parquet(flatDir);
        long factRows = fact.count();

        // ---- 1. CSV vs Parquet ------------------------------------------------------------------
        double csvMb = Locations.sizeOnDisk(csvDir) / 1e6;
        double parquetMb = Locations.sizeOnDisk(Locations.warehouse(out, "fact_trips")) / 1e6;
        Dataset<Row> csv = spark.read().option("header", "true").schema(fact.schema()).csv(csvDir);
        results.add(time("storage_format", "csv", runs, factRows, String.format("%.1f MB on disk", csvMb),
                () -> (long) csv.groupBy("payment_key").agg(sum("total_amount")).collectAsList().size()));
        results.add(time("storage_format", "parquet", runs, factRows, String.format("%.1f MB on disk", parquetMb),
                () -> (long) fact.groupBy("payment_key").agg(sum("total_amount")).collectAsList().size()));

        // ---- 2. Partition pruning ---------------------------------------------------------------
        Row firstMonth = fact.select("year", "month").distinct().orderBy("year", "month").first();
        int y = firstMonth.getInt(0);
        int m = firstMonth.getInt(1);
        Dataset<Row> flat = spark.read().parquet(flatDir);
        String month = String.format("%d-%02d", y, m);
        // A realistic one-month report (several aggregates per zone), so scan cost outweighs job overhead.
        java.util.function.Function<Dataset<Row>, Dataset<Row>> monthReport = df -> df
                .filter(col("year").equalTo(y).and(col("month").equalTo(m)))
                .groupBy("pu_zone_key")
                .agg(count(lit(1)), sum("total_amount"), avg("trip_distance"), avg("duration_min"), sum("tip_amount"));
        // What each layout must read: the whole flat copy vs only that month's folder.
        // The plan's PartitionFilters confirm Spark prunes the other folders.
        String factDir = Locations.warehouse(out, "fact_trips");
        double flatMb = Locations.sizeOnDisk(flatDir) / 1e6;
        double factMb = Locations.sizeOnDisk(factDir) / 1e6;
        double monthMb = Locations.sizeOnDisk(factDir + "/year=" + y + "/month=" + m) / 1e6;
        String plan = monthReport.apply(fact).queryExecution().executedPlan().toString();
        boolean pruned = plan.contains("PartitionFilters: [isnotnull(year");
        results.add(time("partition_pruning", "unpartitioned", runs, factRows,
                String.format("query %s; scans all %.1f MB", month, flatMb),
                () -> (long) monthReport.apply(flat).collectAsList().size()));
        results.add(time("partition_pruning", "partitioned by year/month", runs, factRows,
                String.format("query %s; scans %.1f of %.1f MB%s", month, monthMb, factMb,
                        pruned ? " (partition filter in plan)" : " (no partition filter!)"),
                () -> (long) monthReport.apply(fact).collectAsList().size()));

        // ---- 3. Join strategies -----------------------------------------------------------------
        // Each run builds a fresh query: re-running one Dataset object lets Spark reuse its shuffle files.
        String autoBroadcast = spark.conf().get("spark.sql.autoBroadcastJoinThreshold");
        Supplier<Dataset<Row>> broadcastJoin = () -> fact
                .join(broadcast(zones), fact.col("pu_zone_key").equalTo(zones.col("zone_key")))
                .groupBy("borough").agg(sum("total_amount"));
        results.add(time("join_strategy", "broadcast hash join", runs, factRows, planName(broadcastJoin.get()),
                () -> (long) broadcastJoin.get().collectAsList().size()));

        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", "-1");
        Supplier<Dataset<Row>> mergeJoin = () -> fact
                .join(zones.hint("merge"), fact.col("pu_zone_key").equalTo(zones.col("zone_key")))
                .groupBy("borough").agg(sum("total_amount"));
        results.add(time("join_strategy", "sort-merge join", runs, factRows, planName(mergeJoin.get()),
                () -> (long) mergeJoin.get().collectAsList().size()));
        spark.conf().set("spark.sql.autoBroadcastJoinThreshold", autoBroadcast);

        // ---- 4. RDD vs DataFrame aggregation ----------------------------------------------------
        Dataset<Row> zoneAmounts = fact.select("pu_zone_key", "total_amount");
        results.add(time("aggregation_api", "RDD groupByKey", runs, factRows, "ships every value",
                () -> {
                    JavaPairRDD<Integer, Double> pairs = zoneAmounts.javaRDD()
                            .mapToPair(r -> new Tuple2<>(r.getInt(0), r.getDouble(1)));
                    return (long) pairs.groupByKey().mapValues(vs -> {
                        double s = 0;
                        for (double v : vs) s += v;
                        return s;
                    }).collect().size();
                }));
        results.add(time("aggregation_api", "RDD reduceByKey", runs, factRows, "combines before shuffle",
                () -> (long) zoneAmounts.javaRDD()
                        .mapToPair(r -> new Tuple2<>(r.getInt(0), r.getDouble(1)))
                        .reduceByKey(Double::sum).collect().size()));
        results.add(time("aggregation_api", "DataFrame groupBy", runs, factRows, "Catalyst + columnar scan",
                () -> (long) zoneAmounts.groupBy("pu_zone_key").agg(sum("total_amount")).collectAsList().size()));

        // ---- Report -----------------------------------------------------------------------------
        List<String> lines = new ArrayList<>();
        lines.add("benchmark,variant,median_ms,runs,fact_rows,note");
        for (Result r : results) {
            lines.add(String.format("%s,%s,%.0f,%d,%d,%s", r.benchmark(), r.variant(), r.medianMs(), runs, r.rows(), r.note()));
        }
        Files.write(Locations.report(out, "benchmarks.csv"), lines);

        System.out.printf("%nBenchmarks on %,d trips (median of %d runs, local mode):%n", factRows, runs);
        System.out.printf("  %-18s %-28s %10s   %s%n", "benchmark", "variant", "median ms", "note");
        for (Result r : results) {
            System.out.printf("  %-18s %-28s %10.0f   %s%n", r.benchmark(), r.variant(), r.medianMs(), r.note());
        }
        System.out.println("\nSaved to " + Locations.report(out, "benchmarks.csv"));
    }

    private static Result time(String benchmark, String variant, int runs, long rows, String note, Supplier<Long> job) {
        job.get(); // warm-up: JIT, file listing, metadata caches
        double[] ms = new double[runs];
        for (int i = 0; i < runs; i++) {
            long t = System.nanoTime();
            job.get();
            ms[i] = (System.nanoTime() - t) / 1e6;
        }
        Arrays.sort(ms);
        return new Result(benchmark, variant, ms[runs / 2], rows, note);
    }

    /** Which join operator Spark actually planned, read from the physical plan. */
    private static String planName(Dataset<Row> df) {
        String plan = df.queryExecution().executedPlan().toString();
        if (plan.contains("BroadcastHashJoin")) return "plan: BroadcastHashJoin";
        if (plan.contains("SortMergeJoin")) return "plan: SortMergeJoin";
        if (plan.contains("ShuffledHashJoin")) return "plan: ShuffledHashJoin";
        return "plan: other";
    }
}
