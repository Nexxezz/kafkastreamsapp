package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopicSaver {
    public static String TOPIC_NAME = "join-result-topic";
    public static String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Application").getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("subscribe", TOPIC_NAME)
                .load();
        df.printSchema();
        df.write().parquet(HDFS_PATH);
    }
}
