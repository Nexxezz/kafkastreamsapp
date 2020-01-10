package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopicSaver {
    public static String TOPIC_NAME = "hotels`-topic";
    public static String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/test";
    public static String CHECKPOINT_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/spark_checkpoints";

    public static void main(String[] args) {
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Applicatio1").getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("subscribe", TOPIC_NAME)
                .load();

        df.printSchema();
        df.writeStream().format("parquet").option("checkpointLocation",CHECKPOINT_PATH).start(HDFS_PATH);
    }
}
