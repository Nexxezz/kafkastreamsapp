package spark;

import kafka.data.Weather;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TopicSaver3 {

    public final static Logger LOG = LoggerFactory.getLogger(TopicSaver3.class.getName());

    public static final String TOPIC_NAME = "join-result-topic";
    public static final String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/topicSaverResult";
    public static final int RECORDS_LIMIT = 1000;

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

        final String groupId = UUID.randomUUID().toString().substring(0, 5);

        LOG.info("Starting TopicSaver3 with parameters: topic={}, path={}, limit={}, groupId={}",
                topic, path, limit, groupId);


        final KafkaConsumer<byte[], Weather> consumer = getConsumer(getProps(groupId), topic);
        SparkSession ss = initSpark();

        try {
            int totalRead = 0;
            boolean isTopicEmpty = false;

            while (!isTopicEmpty) {
                final ConsumerRecords<byte[], Weather> consumerRecords = consumer.poll(10000);

                if (consumerRecords.count() == 0) {
                    LOG.debug("no more messages in the topic, messages read total={}", totalRead);
                    isTopicEmpty = true;
                } else {
                    if (LOG.isDebugEnabled()) {
                        consumerRecords.forEach(record -> LOG.debug("key={}, value={}", record.key(), record.value()));
                    }

                    List<Weather> filteredRecords = createStream(consumerRecords.iterator())
                            .map(ConsumerRecord::value)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    saveToHdfsBySpark(filteredRecords, ss, path);

                    totalRead += consumerRecords.count();
                    consumer.commitAsync();
                }
            }
        } finally {
            LOG.debug("final section of try");
            consumer.close();
            ss.stop();
        }

        LOG.info("END");
    }

    private static void saveToHdfsBySpark(List<Weather> records,
                                          SparkSession ss,
                                          String path) {
        JavaSparkContext sparkContext = new JavaSparkContext(ss.sparkContext());

        JavaRDD<Weather> rdd = sparkContext.parallelize(records);
        Dataset<Row> df = ss.createDataFrame(rdd, Weather.class);
        df.write().mode(SaveMode.Append).parquet(path);
    }

    private static SparkSession initSpark() {
        return SparkSession.builder()
                .master("local[*]") //.master("yarn")
                .appName("TopicSaver3-spark")
                .getOrCreate();
    }

    private static KafkaConsumer<byte[], Weather> getConsumer(Properties props, String topic) {
        KafkaConsumer<byte[], Weather> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }


    private static <T> Stream<T> createStream(Iterator<T> iterator) {
        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private static Properties getProps(String groupId) {
        Properties props = new Properties();
        props.put("group.id", groupId);
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        props.put("max.poll.records", 1000);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "kafka.serdes.weather.JsonWeatherDeserializer");

        //not a good idea, see
        //https://stackoverflow.com/questions/28561147/how-to-read-data-using-kafka-consumer-api-from-beginning
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "topic-saver3-id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "30000");
        return props;
    }

}
