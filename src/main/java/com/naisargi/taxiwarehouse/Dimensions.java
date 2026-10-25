package com.naisargi.taxiwarehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/** The five dimension tables of the star schema. */
public final class Dimensions {
    private Dimensions() { }

    /** One row per calendar day between the first and last trip. */
    public static void writeDate(SparkSession spark, String out, String firstDay, String lastDay) {
        Dataset<Row> days = spark.sql(String.format(
                "SELECT explode(sequence(DATE'%s', DATE'%s', INTERVAL 1 DAY)) AS date", firstDay, lastDay));
        Dataset<Row> dim = days.select(
                date_format(col("date"), "yyyyMMdd").cast("int").as("date_key"),
                col("date"),
                year(col("date")).as("year"),
                quarter(col("date")).as("quarter"),
                month(col("date")).as("month"),
                date_format(col("date"), "MMM").as("month_name"),
                dayofmonth(col("date")).as("day"),
                date_format(col("date"), "EEEE").as("day_name"),
                dayofweek(col("date")).isin(1, 7).as("is_weekend"));
        save(dim.coalesce(1), out, "dim_date");
    }

    /** 24 hours grouped into bands that analysts actually ask about. */
    public static void writeTime(SparkSession spark, String out) {
        List<Row> rows = new ArrayList<>();
        for (int h = 0; h < 24; h++) {
            String band;
            if (h < 6) band = "Night (0-5)";
            else if (h < 10) band = "Morning peak (6-9)";
            else if (h < 16) band = "Midday (10-15)";
            else if (h < 20) band = "Evening peak (16-19)";
            else band = "Late evening (20-23)";
            rows.add(RowFactory.create(h, String.format("%02d:00", h), band));
        }
        StructType schema = new StructType()
                .add("hour_key", DataTypes.IntegerType, false)
                .add("hour_label", DataTypes.StringType, false)
                .add("time_band", DataTypes.StringType, false);
        save(spark.createDataFrame(rows, schema).coalesce(1), out, "dim_time");
    }

    /** From the TLC taxi_zone_lookup.csv (LocationID, Borough, Zone, service_zone). */
    public static void writeZone(SparkSession spark, String out, String zonesCsv) {
        Dataset<Row> dim = spark.read().option("header", "true").csv(zonesCsv).select(
                col("LocationID").cast("int").as("zone_key"),
                coalesce(col("Borough"), lit("Unknown")).as("borough"),
                coalesce(col("Zone"), lit("Unknown")).as("zone"),
                coalesce(col("service_zone"), lit("Unknown")).as("service_zone"));
        save(dim.coalesce(1), out, "dim_zone");
    }

    /** Payment codes from the TLC yellow-taxi data dictionary. */
    public static void writePayment(SparkSession spark, String out) {
        Object[][] codes = {
                {0, "Flex fare"}, {1, "Credit card"}, {2, "Cash"}, {3, "No charge"},
                {4, "Dispute"}, {5, "Unknown"}, {6, "Voided trip"}};
        save(codeTable(spark, codes, "payment_key", "payment_name"), out, "dim_payment");
    }

    /** Vendor codes from the TLC yellow-taxi data dictionary; 0 = missing in the raw data. */
    public static void writeVendor(SparkSession spark, String out) {
        Object[][] codes = {
                {0, "Unknown"}, {1, "Creative Mobile Technologies"}, {2, "Curb Mobility"},
                {6, "Myle Technologies"}, {7, "Helix"}};
        save(codeTable(spark, codes, "vendor_key", "vendor_name"), out, "dim_vendor");
    }

    private static Dataset<Row> codeTable(SparkSession spark, Object[][] codes, String keyCol, String nameCol) {
        List<Row> rows = new ArrayList<>();
        for (Object[] c : codes) {
            rows.add(RowFactory.create(c[0], c[1]));
        }
        StructType schema = new StructType()
                .add(keyCol, DataTypes.IntegerType, false)
                .add(nameCol, DataTypes.StringType, false);
        return spark.createDataFrame(rows, schema).coalesce(1);
    }

    private static void save(Dataset<Row> dim, String out, String name) {
        dim.write().mode(SaveMode.Overwrite).parquet(Locations.warehouse(out, name));
    }
}
