package com.naisargi.taxiwarehouse;

import org.apache.spark.sql.SparkSession;

/** One place for Spark settings, tuned for a laptop in local mode. */
public final class SparkFactory {
    private SparkFactory() { }

    public static SparkSession create(String appName, String master) {
        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master(master)
                // 200 shuffle partitions (the default) is far too many for one machine.
                .config("spark.sql.shuffle.partitions", "8")
                // Trip timestamps are local NYC wall-clock times; keep them as written.
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }
}
