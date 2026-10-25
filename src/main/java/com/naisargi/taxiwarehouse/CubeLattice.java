package com.naisargi.taxiwarehouse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds the whole lattice of cuboids for (borough, time band, payment) with plain RDDs,
 * then checks every cell against the cube Spark SQL produced in the olap step.
 *
 * Trick: each trip is emitted once per cuboid, i.e. 2^3 = 8 times, with "*" in the dimensions
 * that cuboid aggregates away. One reduceByKey (one shuffle) then computes all 8 cuboids at once.
 */
public final class CubeLattice {
    private CubeLattice() { }

    private static final int DIMS = 3;
    private static final String ALL = "*";

    public static void run(SparkSession spark, Args args) {
        String out = args.get("out", "out");
        Olap.registerViews(spark, out);

        JavaRDD<Row> trips = spark.sql(
                "SELECT pu_borough, time_band, payment_name, total_amount FROM trips_enriched").javaRDD();

        // (cellKey, (revenue, trips)) for every trip in every cuboid.
        JavaPairRDD<String, Tuple2<Double, Long>> emitted = trips.flatMapToPair(r -> {
            String[] dims = {r.getString(0), r.getString(1), r.getString(2)};
            double amount = r.getDouble(3);
            List<Tuple2<String, Tuple2<Double, Long>>> outRows = new ArrayList<>(1 << DIMS);
            for (int mask = 0; mask < (1 << DIMS); mask++) {
                outRows.add(new Tuple2<>(cellKey(dims, mask), new Tuple2<>(amount, 1L)));
            }
            return outRows.iterator();
        });

        JavaPairRDD<String, Tuple2<Double, Long>> lattice = emitted.reduceByKey(
                (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()));

        Map<String, Tuple2<Double, Long>> rddCells = lattice.collectAsMap();

        // The same cells from the materialised Spark SQL cube (run `olap` first).
        List<Row> sqlRows = spark.read().parquet(Locations.warehouse(out, "trip_cube"))
                .select("pu_borough", "time_band", "payment_name",
                        "g_borough", "g_band", "g_payment", "trips", "revenue")
                .collectAsList();

        int checked = 0;
        int mismatches = 0;
        for (Row r : sqlRows) {
            String[] dims = {
                    r.getByte(3) == 1 ? ALL : r.getString(0),
                    r.getByte(4) == 1 ? ALL : r.getString(1),
                    r.getByte(5) == 1 ? ALL : r.getString(2)};
            String key = String.join("|", dims);
            Tuple2<Double, Long> mine = rddCells.get(key);
            long sqlTrips = r.getLong(6);
            double sqlRevenue = r.getDouble(7);
            boolean ok = mine != null && mine._2() == sqlTrips && Math.abs(mine._1() - sqlRevenue) < 0.01 * Math.max(1, sqlTrips);
            if (!ok) {
                mismatches++;
                if (mismatches <= 5) {
                    System.out.printf("  mismatch %s: rdd=%s sql=(%.2f, %d)%n", key, mine, sqlRevenue, sqlTrips);
                }
            }
            checked++;
        }

        System.out.printf("%nRDD lattice: %d cuboids, %,d cells. Spark SQL cube: %,d cells.%n",
                1 << DIMS, rddCells.size(), sqlRows.size());
        Tuple2<Double, Long> apex = rddCells.get(String.join("|", ALL, ALL, ALL));
        System.out.printf("Apex cell (*|*|*): %,d trips, revenue %,.2f%n", apex._2(), apex._1());
        if (mismatches == 0 && checked == rddCells.size()) {
            System.out.println("Check passed: every cell matches.");
        } else {
            System.out.printf("Check FAILED: %d of %d cells differ (RDD has %d cells).%n",
                    mismatches, checked, rddCells.size());
            System.exit(1);
        }
    }

    /** Bit i of the mask set = dimension i aggregated away (shown as "*"), like Spark's grouping_id. */
    static String cellKey(String[] dims, int mask) {
        String[] k = new String[dims.length];
        for (int i = 0; i < dims.length; i++) {
            boolean aggregated = ((mask >> (dims.length - 1 - i)) & 1) == 1;
            k[i] = aggregated ? ALL : dims[i];
        }
        return String.join("|", k);
    }
}
