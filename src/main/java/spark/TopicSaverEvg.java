package spark;

import kafka.data.HotelWeather;
import org.apache.spark.sql.*;

public class TopicSaverEvg {

    public static final String DEFAULT_TOPIC_NAME = "join-result-topic";
    public static final String DEFAULT_HDFS_OUTPUT_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/dataset";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Streaming App Example")
                .getOrCreate();

        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
                .option("subscribe", DEFAULT_TOPIC_NAME)
                .load();

        Encoder<HotelWeather> encoder = Encoders.bean(HotelWeather.class);
        Dataset<HotelWeather> hotelWeatherDataset = ds.as(encoder);
        hotelWeatherDataset.printSchema();
        System.out.println(hotelWeatherDataset.count());
        hotelWeatherDataset.write().parquet(DEFAULT_HDFS_OUTPUT_PATH);
    }
}
