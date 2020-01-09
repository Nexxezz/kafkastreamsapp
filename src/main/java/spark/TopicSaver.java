package spark;
public class TopicSaver {
    public static void main(String[] args) {
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("subscribe", "join-result-topic")
                .load();
    }
}
