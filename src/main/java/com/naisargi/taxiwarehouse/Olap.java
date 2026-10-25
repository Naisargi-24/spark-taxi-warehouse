package com.naisargi.taxiwarehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Analytical queries over the star schema, written in Spark SQL:
 * ROLLUP, a materialised CUBE with slice and dice on it, window ranking, and two business questions.
 * Each result is printed and saved as CSV under out/olap/.
 */
public final class Olap {
    private Olap() { }

    static final String[] TABLES = {"fact_trips", "dim_date", "dim_time", "dim_zone", "dim_payment", "dim_vendor"};

    public static void run(SparkSession spark, Args args) {
        String out = args.get("out", "out");
        registerViews(spark, out);

        // Roll-up: month totals plus a grand total, in one statement.
        query(spark, out, "monthly_revenue_rollup", """
                SELECT year, month,
                       COUNT(*)                       AS trips,
                       CAST(SUM(total_amount) AS DECIMAL(16, 2)) AS revenue,
                       ROUND(AVG(total_amount), 2)    AS avg_total,
                       GROUPING_ID()                  AS gid
                FROM fact_trips
                GROUP BY ROLLUP (year, month)
                ORDER BY year NULLS LAST, month NULLS LAST""");

        // Cube over three dimensions = 2^3 = 8 GROUP BYs. Materialise it once, then answer from it.
        Dataset<Row> cube = spark.sql("""
                SELECT pu_borough, time_band, payment_name,
                       GROUPING(pu_borough)   AS g_borough,
                       GROUPING(time_band)    AS g_band,
                       GROUPING(payment_name) AS g_payment,
                       COUNT(*)                    AS trips,
                       ROUND(SUM(total_amount), 2) AS revenue,
                       ROUND(SUM(tip_amount), 2)   AS tips
                FROM trips_enriched
                GROUP BY CUBE (pu_borough, time_band, payment_name)""");
        cube.coalesce(1).write().mode(SaveMode.Overwrite).parquet(Locations.warehouse(out, "trip_cube"));
        spark.read().parquet(Locations.warehouse(out, "trip_cube")).createOrReplaceTempView("trip_cube");
        System.out.printf("%nMaterialised trip_cube: %,d cells%n",
                spark.table("trip_cube").count());

        // Slice: fix one dimension (borough = Manhattan), all payment types.
        query(spark, out, "slice_manhattan_by_band", """
                SELECT time_band, trips, revenue
                FROM trip_cube
                WHERE pu_borough = 'Manhattan' AND g_band = 0 AND g_payment = 1
                ORDER BY revenue DESC""");

        // Dice: a sub-cube on all three dimensions.
        query(spark, out, "dice_peaks_two_boroughs", """
                SELECT pu_borough, time_band, payment_name, trips, revenue
                FROM trip_cube
                WHERE g_borough = 0 AND g_band = 0 AND g_payment = 0
                  AND pu_borough IN ('Manhattan', 'Brooklyn')
                  AND time_band IN ('Morning peak (6-9)', 'Evening peak (16-19)')
                  AND payment_name IN ('Credit card', 'Cash')
                ORDER BY pu_borough, time_band, payment_name""");

        // Window function: top 3 pickup zones by revenue inside each borough.
        query(spark, out, "top_zones_per_borough", """
                SELECT pu_borough, pu_zone, trips, revenue, rnk
                FROM (
                    SELECT pu_borough, pu_zone,
                           COUNT(*) AS trips,
                           ROUND(SUM(total_amount), 2) AS revenue,
                           RANK() OVER (PARTITION BY pu_borough ORDER BY SUM(total_amount) DESC) AS rnk
                    FROM trips_enriched
                    GROUP BY pu_borough, pu_zone)
                WHERE rnk <= 3
                ORDER BY pu_borough, rnk""");

        // Tips are only recorded for card payments, so measure tip rate on card trips.
        query(spark, out, "card_tip_rate_by_hour", """
                SELECT hour_key AS hour,
                       COUNT(*) AS trips,
                       ROUND(100 * SUM(tip_amount) / SUM(fare_amount), 1) AS tip_pct_of_fare
                FROM trips_enriched
                WHERE payment_name = 'Credit card'
                GROUP BY hour_key
                ORDER BY hour_key""");

        query(spark, out, "weekday_vs_weekend", """
                SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
                       COUNT(*) AS trips,
                       ROUND(AVG(duration_min), 1) AS avg_minutes,
                       ROUND(AVG(trip_distance), 2) AS avg_miles,
                       ROUND(SUM(trip_distance) / (SUM(duration_min) / 60), 1) AS avg_mph
                FROM trips_enriched
                GROUP BY is_weekend
                ORDER BY day_type""");
    }

    /** Registers every warehouse table, plus trips_enriched = the fact joined to its dimensions. */
    static void registerViews(SparkSession spark, String out) {
        for (String t : TABLES) {
            spark.read().parquet(Locations.warehouse(out, t)).createOrReplaceTempView(t);
        }
        spark.sql("""
                CREATE OR REPLACE TEMPORARY VIEW trips_enriched AS
                SELECT f.*,
                       d.date, d.month_name, d.day_name, d.is_weekend,
                       t.time_band,
                       z.borough AS pu_borough, z.zone AS pu_zone,
                       p.payment_name
                FROM fact_trips f
                JOIN dim_date    d ON f.date_key    = d.date_key
                JOIN dim_time    t ON f.hour_key    = t.hour_key
                JOIN dim_zone    z ON f.pu_zone_key = z.zone_key
                JOIN dim_payment p ON f.payment_key = p.payment_key""");
    }

    private static void query(SparkSession spark, String out, String name, String sql) {
        Dataset<Row> result = spark.sql(sql);
        System.out.println("\n== " + name + " ==");
        result.show(30, false);
        result.coalesce(1).write().mode(SaveMode.Overwrite)
                .option("header", "true").csv(Locations.olap(out, name));
    }
}
