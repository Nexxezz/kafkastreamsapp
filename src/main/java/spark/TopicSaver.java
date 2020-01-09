package spark;
public class TopicSaver {
    public static String TOPIC_NAME = "join-result-topic";
    public static String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/dataset";
    public static void main(String[] args) {
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("subscribe", TOPIC_NAME)
                .load();
        df.write.parquet(HDFS_PATH);
    }
}
