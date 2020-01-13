package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicSaver {

    public static Logger LOG = LoggerFactory.getLogger(TopicSaver.class.getName());

    public static final String TOPIC_NAME = "hotels-topic";
    public static final String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/test";
    public static final String CHECKPOINT_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/spark_checkpoints";

    public static void main(String[] args) {
        String topic;
        String path;

        if (args.length == 0) {
            topic = TOPIC_NAME;
            path = HDFS_PATH;
        } else {
            topic = args[0];
            path = args[1];
        }

        LOG.info("TopicSaver started with topic={}, output path={}", topic, path);

//        SparkSession.clearActiveSession();
//        SparkSession.clearDefaultSession();

        SparkSession spark = SparkSession.builder()
//                .master("yarn")
                .master("local[*]")
                .appName("topic-saver-app")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("subscribe", topic)
                .load();

        LOG.info("df.schema={}", df.schema().simpleString());
//        LOG.info("df.count={}", df.count());

        df.writeStream()
                .format("parquet")
                .option("checkpointLocation", CHECKPOINT_PATH)
                .start(path);

        spark.stop();
    }
}

// to case class example
//        Dataset<HotelWeather> hotelWeatherDataset = ds.as(Encoders.bean(HotelWeather.class));
//        hotelWeatherDataset.printSchema();
//        System.out.println(hotelWeatherDataset.count());
//        hotelWeatherDataset.write().parquet(path);
