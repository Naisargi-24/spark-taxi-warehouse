package com.naisargi.taxiwarehouse;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.nio.file.Files;
import java.time.YearMonth;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Raw TLC trip files -> cleaned trips -> a star schema in Parquet:
 *
 *   fact_trips (partitioned by year, month)
 *     date_key -> dim_date, hour_key -> dim_time, pu/do_zone_key -> dim_zone,
 *     payment_key -> dim_payment, vendor_key -> dim_vendor
 *
 * Every row that fails a cleaning rule is counted per rule and written to reports/data_quality.csv.
 */
public final class Etl {
    private Etl() { }

    public static void run(SparkSession spark, Args args) throws IOException {
        String raw = args.get("raw", "data/raw");
        String zones = args.get("zones", "data/taxi_zone_lookup.csv");
        String out = args.get("out", "out");
        String fromArg = args.getOrNull("from");
        String toArg = args.getOrNull("to");

        long t0 = System.nanoTime();

        // 1. Extract: read every Parquet file under --raw (the TLC files or the sample's output).
        //    Column types drift between years (e.g. passenger_count is double in older files), so cast.
        Dataset<Row> trips = spark.read()
                .option("recursiveFileLookup", "true")
                .parquet(raw)
                .select(
                        col("VendorID").cast("int").as("vendor_id"),
                        col("tpep_pickup_datetime").cast("timestamp").as("pickup_ts"),
                        col("tpep_dropoff_datetime").cast("timestamp").as("dropoff_ts"),
                        col("passenger_count").cast("int").as("passenger_count"),
                        col("trip_distance").cast("double").as("trip_distance"),
                        col("PULocationID").cast("int").as("pu_location_id"),
                        col("DOLocationID").cast("int").as("do_location_id"),
                        col("payment_type").cast("int").as("payment_type"),
                        col("fare_amount").cast("double").as("fare_amount"),
                        col("tip_amount").cast("double").as("tip_amount"),
                        col("total_amount").cast("double").as("total_amount"))
                .withColumn("duration_min",
                        unix_timestamp(col("dropoff_ts")).minus(unix_timestamp(col("pickup_ts"))).divide(60.0));

        // 2. Transform: one boolean column per cleaning rule, so a single pass can count them all.
        //    Real monthly files contain a few trips dated years away; --from/--to drops them.
        String lo = fromArg != null ? YearMonth.parse(fromArg).atDay(1).toString() : "2009-01-01";
        String hi = toArg != null ? YearMonth.parse(toArg).plusMonths(1).atDay(1).toString() : "2031-01-01";

        Column badTime = col("pickup_ts").isNull().or(col("dropoff_ts").isNull())
                .or(col("pickup_ts").lt(to_timestamp(lit(lo)))).or(col("pickup_ts").geq(to_timestamp(lit(hi))));
        Column badDuration = col("duration_min").isNull()
                .or(col("duration_min").lt(1)).or(col("duration_min").gt(180));
        Column badDistance = col("trip_distance").isNull()
                .or(col("trip_distance").leq(0)).or(col("trip_distance").gt(100));
        Column badAmount = col("total_amount").isNull().or(col("fare_amount").isNull())
                .or(col("fare_amount").lt(0)).or(col("total_amount").lt(0)).or(col("total_amount").gt(1000));
        Column badZone = col("pu_location_id").isNull().or(col("do_location_id").isNull())
                .or(not(col("pu_location_id").between(1, 265))).or(not(col("do_location_id").between(1, 265)));

        Dataset<Row> flagged = trips
                .withColumn("bad_time", badTime)
                .withColumn("bad_duration", coalesce(badDuration, lit(true)))
                .withColumn("bad_distance", badDistance)
                .withColumn("bad_amount", badAmount)
                .withColumn("bad_zone", badZone)
                .persist(StorageLevel.MEMORY_AND_DISK());

        Row q = flagged.agg(
                count(lit(1)),
                sum(when(col("bad_time"), 1).otherwise(0)),
                sum(when(col("bad_duration"), 1).otherwise(0)),
                sum(when(col("bad_distance"), 1).otherwise(0)),
                sum(when(col("bad_amount"), 1).otherwise(0)),
                sum(when(col("bad_zone"), 1).otherwise(0)),
                sum(when(col("bad_time").or(col("bad_duration")).or(col("bad_distance"))
                        .or(col("bad_amount")).or(col("bad_zone")), 1).otherwise(0))
        ).first();

        Dataset<Row> clean = flagged.filter(not(col("bad_time")).and(not(col("bad_duration")))
                .and(not(col("bad_distance"))).and(not(col("bad_amount"))).and(not(col("bad_zone"))));

        // 3. Load: the fact table, partitioned so a query for one month reads one folder.
        Dataset<Row> fact = clean.select(
                date_format(col("pickup_ts"), "yyyyMMdd").cast("int").as("date_key"),
                hour(col("pickup_ts")).as("hour_key"),
                col("pu_location_id").as("pu_zone_key"),
                col("do_location_id").as("do_zone_key"),
                coalesce(col("payment_type"), lit(5)).as("payment_key"),
                coalesce(col("vendor_id"), lit(0)).as("vendor_key"),
                col("passenger_count"),
                round(col("trip_distance"), 2).as("trip_distance"),
                round(col("duration_min"), 2).as("duration_min"),
                col("fare_amount"),
                col("tip_amount"),
                col("total_amount"),
                year(col("pickup_ts")).as("year"),
                month(col("pickup_ts")).as("month"));

        // Shuffle by day: each month lands in several files (one per shuffle partition holding its days),
        // so a one-month query still reads in parallel. Partitioning by (year, month) alone would give
        // one file per month and a single-core scan.
        fact.repartition(col("date_key"))
                .write().mode(SaveMode.Overwrite)
                .partitionBy("year", "month")
                .parquet(Locations.warehouse(out, "fact_trips"));

        Row range = clean.agg(min(to_date(col("pickup_ts"))), max(to_date(col("pickup_ts")))).first();
        Dimensions.writeDate(spark, out, range.getDate(0).toString(), range.getDate(1).toString());
        Dimensions.writeTime(spark, out);
        Dimensions.writeZone(spark, out, zones);
        Dimensions.writePayment(spark, out);
        Dimensions.writeVendor(spark, out);

        flagged.unpersist();
        double secs = (System.nanoTime() - t0) / 1e9;

        long total = q.getLong(0);
        long rejected = q.getLong(6);
        List<String> report = List.of(
                "metric,rows,share_pct",
                line("raw_rows", total, total),
                line("bad_time", q.getLong(1), total),
                line("bad_duration", q.getLong(2), total),
                line("bad_distance", q.getLong(3), total),
                line("bad_amount", q.getLong(4), total),
                line("bad_zone", q.getLong(5), total),
                line("rejected_any_rule", rejected, total),
                line("loaded_rows", total - rejected, total));
        Files.write(Locations.report(out, "data_quality.csv"), report);

        System.out.println("\nData quality (a row can break several rules):");
        report.forEach(l -> System.out.println("  " + l.replace(",", "\t")));
        System.out.printf("%nLoaded %,d of %,d trips into %s in %.1f s%n",
                total - rejected, total, Locations.warehouse(out, ""), secs);
        System.out.printf("Fact table on disk: %.1f MB%n",
                Locations.sizeOnDisk(Locations.warehouse(out, "fact_trips")) / 1e6);
    }

    private static String line(String metric, long rows, long total) {
        return String.format("%s,%d,%.2f", metric, rows, total == 0 ? 0.0 : 100.0 * rows / total);
    }
}
