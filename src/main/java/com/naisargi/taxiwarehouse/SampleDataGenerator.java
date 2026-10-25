package com.naisargi.taxiwarehouse;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Generates synthetic trips with the same columns and types as the NYC TLC yellow-taxi Parquet
 * files, plus a matching zone lookup CSV. About 2% of rows are deliberately dirty so the ETL's
 * cleaning rules have something to reject. Use this to test the pipeline; use real data for results.
 */
public final class SampleDataGenerator {
    private SampleDataGenerator() { }

    public static void run(SparkSession spark, Args args) throws IOException {
        String out = args.get("out", "data/sample");
        long rows = Long.parseLong(args.get("rows", "2000000"));
        YearMonth from = YearMonth.parse(args.get("from", "2024-01"));
        int months = args.getInt("months", 3);

        long start = from.atDay(1).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        long end = from.plusMonths(months).atDay(1).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        long span = end - start;

        Column u1 = rand(11), u2 = rand(12), u3 = rand(13), u4 = rand(14), u5 = rand(15), dirt = rand(16);

        Dataset<Row> base = spark.range(rows)
                .withColumn("pickup_sec", lit(start).plus(floor(u1.multiply(span))))
                // Mostly short trips with a long tail: 2 + |N(0,1)| * 14 minutes.
                .withColumn("dur_min", lit(2.0).plus(abs(randn(17)).multiply(14)))
                .withColumn("pu", floor(pow(u2, 2.2).multiply(263)).plus(1).cast("int"))
                .withColumn("do", floor(pow(u3, 1.8).multiply(263)).plus(1).cast("int"))
                .withColumn("pay", when(u4.lt(0.70), 1).when(u4.lt(0.95), 2).when(u4.lt(0.97), 3)
                        .when(u4.lt(0.98), 4).otherwise(0))
                .withColumn("dist", col("dur_min").multiply(0.22).multiply(lit(0.6).plus(u5)))
                .withColumn("dirt", dirt);

        Column fare = round(lit(3.0).plus(col("dist").multiply(2.5)).plus(col("dur_min").multiply(0.5)), 2);
        Column tip = when(col("pay").equalTo(1), round(fare.multiply(rand(18).multiply(0.35)), 2)).otherwise(0.0);
        Column pickup = timestamp_seconds(col("pickup_sec"));
        // Dirty rows: dropoff before pickup, zero distance, negative fare.
        Column dropoffSec = when(col("dirt").lt(0.008), col("pickup_sec").minus(600))
                .otherwise(col("pickup_sec").plus(round(col("dur_min").multiply(60)).cast("long")));
        Column distance = when(col("dirt").between(0.008, 0.014), 0.0).otherwise(round(col("dist"), 2));
        Column fareFinal = when(col("dirt").between(0.014, 0.02), fare.multiply(-1)).otherwise(fare);

        Dataset<Row> trips = base.select(
                when(rand(19).lt(0.3), 1).otherwise(2).cast("int").as("VendorID"),
                pickup.cast("timestamp_ntz").as("tpep_pickup_datetime"),
                timestamp_seconds(dropoffSec).cast("timestamp_ntz").as("tpep_dropoff_datetime"),
                when(rand(20).lt(0.72), 1).when(rand(21).lt(0.7), 2).otherwise(3).cast("long").as("passenger_count"),
                distance.cast("double").as("trip_distance"),
                lit(1L).as("RatecodeID"),
                lit("N").as("store_and_fwd_flag"),
                col("pu").as("PULocationID"),
                col("do").as("DOLocationID"),
                col("pay").cast("long").as("payment_type"),
                fareFinal.cast("double").as("fare_amount"),
                lit(1.0).as("extra"),
                lit(0.5).as("mta_tax"),
                tip.cast("double").as("tip_amount"),
                lit(0.0).as("tolls_amount"),
                lit(1.0).as("improvement_surcharge"),
                round(fareFinal.plus(tip).plus(lit(5.0)), 2).cast("double").as("total_amount"),
                lit(2.5).as("congestion_surcharge"),
                lit(0.0).as("Airport_fee"));

        trips.write().mode(SaveMode.Overwrite).parquet(out + "/raw");
        writeZoneLookup(Path.of(out, "taxi_zone_lookup.csv"));

        System.out.printf("Wrote %,d synthetic trips (%s to %s) to %s/raw and the zone lookup to %s%n",
                rows, from, from.plusMonths(months - 1), out, out + "/taxi_zone_lookup.csv");
    }

    /** 265 zones in the real file's layout; boroughs assigned by ID range. */
    private static void writeZoneLookup(Path file) throws IOException {
        Files.createDirectories(file.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("\"LocationID\",\"Borough\",\"Zone\",\"service_zone\"");
        for (int id = 1; id <= 265; id++) {
            String borough;
            String service;
            if (id == 1) { borough = "EWR"; service = "EWR"; }
            else if (id <= 90) { borough = "Manhattan"; service = id <= 60 ? "Yellow Zone" : "Boro Zone"; }
            else if (id <= 160) { borough = "Brooklyn"; service = "Boro Zone"; }
            else if (id <= 220) { borough = "Queens"; service = id == 161 || id == 162 ? "Airports" : "Boro Zone"; }
            else if (id <= 250) { borough = "Bronx"; service = "Boro Zone"; }
            else if (id <= 263) { borough = "Staten Island"; service = "Boro Zone"; }
            else { borough = "Unknown"; service = "N/A"; }
            lines.add(String.format("%d,\"%s\",\"%s Zone %d\",\"%s\"", id, borough, borough, id, service));
        }
        Files.write(file, lines);
    }
}
