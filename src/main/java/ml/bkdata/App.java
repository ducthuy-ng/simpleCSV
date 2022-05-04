package ml.bkdata;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple CSV").getOrCreate();
        Dataset<Row> csvFile = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("/Users/ducth/apache-ecosystem/data-pipeline/sensor/Measurement_summary.csv");

        System.out.println(csvFile.count());
        csvFile.describe().show();
        System.out.println(csvFile.head());

        spark.stop();

    }
}