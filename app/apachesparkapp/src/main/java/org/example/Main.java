package org.example;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("ApacheSparkApp").getOrCreate();

        // https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html
        Dataset<Row> ds = spark.read().option("header", true).csv(args[0]);

        // Show schema
//        ds.printSchema();

        // Count rows
//        ds.count();

        // Get unique values
//        ds.select(col("AREA NAME")).distinct().show();

        // Aggregate
//        ds.groupBy(col("AREA NAME"))
//            .agg(max("DATE OCC").as("LatestDate"),
//                    count("*").alias("Count"))
//            .select(col("AREA NAME"),
//                    col("LatestDate"),
//                    col("Count")).show();

        // Export
//        ds.write().csv("/opt/spark/results");

        spark.stop();
    }
}