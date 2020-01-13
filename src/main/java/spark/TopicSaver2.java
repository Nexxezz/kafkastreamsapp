package spark;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Save Kafka topic to HDFS using Kafka Streams API and Spark
 */
public class TopicSaver2 {

    public static Logger LOG = LoggerFactory.getLogger(TopicSaver2.class.getName());

    public static final String TOPIC_NAME = "hotels-topic";
    public static final String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/test";
    public static final int RECORDS_LIMIT = 100;

    public static void main(String[] args) {
        String topic;
        String path;
        int limit;

        if (args.length == 0) {
            topic = TOPIC_NAME;
            path = HDFS_PATH;
            limit = RECORDS_LIMIT;
        } else {
            topic = args[0];
            path = args[1];
            limit = Integer.parseInt(args[2]);
        }

        LOG.info("TopicSaver2 started with parameters: \ntopic={}, \noutput path={}, \nlimit={}", topic, path, limit);

        List<String> buff = new ArrayList<>(limit);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<byte[], byte[]> kstream = builder.stream(topic);

        AtomicInteger recordsCount = new AtomicInteger(0);
        kstream.map((key, value) -> {
            if (recordsCount.incrementAndGet() <= limit) toBuff(key, value, buff);
            return KeyValue.pair(key, value);
        });

        proceedStreams(builder);
        saveToHdfs(buff, path);

        LOG.info("END");
    }

    private static void toBuff(byte[] key, byte[] value, List<String> buff) {
        String keyStr;
        if (key == null) keyStr = "NULL-KEY";
        else if (key.length == 0) keyStr = "EMPTY-KEY";
        else keyStr = new String(key);

        String valueStr;
        if (value == null) valueStr = "NULL-VALUE";
        else if (value.length == 0) valueStr = "EMPTY-VALUE";
        else {
            valueStr = new String(value);
            if (valueStr.startsWith("{") && valueStr.endsWith("}"))
                buff.add(valueStr);
            else
                LOG.warn("Found non-JSON record in topic: {}", valueStr);
        }
        LOG.debug("key={} value={}", keyStr, valueStr);
    }


    protected static void saveToHdfs(List<String> buff, String path) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]") //.master("yarn")
                .appName("topic2-saver-app")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rdd = sparkContext.parallelize(buff);

        Dataset<Row> ds = spark.read().json(rdd);
        ds.show();
        ds.write().parquet(path);
    }

    private static void proceedStreams(StreamsBuilder builder) {
        Properties props = getProperties();
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getProperties() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return streamsConfiguration;
    }
}
