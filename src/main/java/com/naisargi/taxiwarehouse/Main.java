package com.naisargi.taxiwarehouse;

import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/** Entry point: ./run.sh <command> [--option value ...] */
public final class Main {

    private static final String USAGE = String.join("\n",
            "Usage: ./run.sh <command> [options]",
            "",
            "Commands:",
            "  sample   Generate synthetic trips in the NYC TLC schema (for a quick test without downloads)",
            "           --out data/sample  --rows 2000000  --from 2024-01  --months 3",
            "  etl      Clean raw trips and build the star schema in Parquet",
            "           --raw data/raw  --zones data/taxi_zone_lookup.csv  --out out  [--from 2024-01 --to 2024-03]",
            "  olap     Run the analytical queries and materialise the trip cube",
            "           --out out",
            "  lattice  Compute the cube lattice with RDDs and check it against Spark's CUBE",
            "           --out out",
            "  bench    Storage, partition-pruning, join-strategy and RDD-vs-DataFrame benchmarks",
            "           --out out  [--runs 3]",
            "",
            "Common: --master local[*]");

    private Main() { }

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0 || argv[0].equals("-h") || argv[0].equals("--help")) {
            System.out.println(USAGE);
            return;
        }
        String command = argv[0];
        Args args = Args.parse(Arrays.copyOfRange(argv, 1, argv.length));

        SparkSession spark = SparkFactory.create("taxi-warehouse-" + command, args.get("master", "local[*]"));
        try {
            switch (command) {
                case "sample" -> SampleDataGenerator.run(spark, args);
                case "etl" -> Etl.run(spark, args);
                case "olap" -> Olap.run(spark, args);
                case "lattice" -> CubeLattice.run(spark, args);
                case "bench" -> Benchmarks.run(spark, args);
                default -> {
                    System.err.println("Unknown command: " + command + "\n\n" + USAGE);
                    System.exit(2);
                }
            }
        } finally {
            spark.stop();
        }
    }
}
