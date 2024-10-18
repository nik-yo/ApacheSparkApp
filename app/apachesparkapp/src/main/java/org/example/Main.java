package org.example;
import org.apache.spark.sql.Column;
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
        ds.select(col("make_name")).distinct().show();

        // Aggregate
//        ds.groupBy(col("AREA NAME"))
//            .agg(max("DATE OCC").as("LatestDate"),
//                    count("*").alias("Count"))
//            .select(col("AREA NAME"),
//                    col("LatestDate"),
//                    col("Count")).show();

        Column makeName = col("make_name");
        Column modelName = col("model_name");
        Column count = col("count");

        ds = ds.groupBy(makeName, modelName)
                .agg(count("*").alias("count"))
                        .select(makeName, modelName, count)
                        .sort(desc("count"));
        // Export
        ds.write().mode("overwrite").option("compression","gzip").csv("/tmp/spark/results");

        spark.stop();
    }
}