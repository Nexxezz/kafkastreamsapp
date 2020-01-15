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

    private static final int KAFKA_MAX_POLL_RECORDS = 1000;
    private static final int KAFKA_POLL_TIMEOUT_MS = 10000;
    private static final int RECORDS_BUFFER_SIZE = 100_000;

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

        final String groupId = UUID.randomUUID().toString().substring(0, 5);

        LOG.info("Starting TopicSaver3 with parameters: topic={}, path={}, groupId={}",
                topic, path, groupId);

        try (SparkSession ss = initSpark();
             KafkaConsumer<byte[], Weather> consumer = getConsumer(getProps(groupId), topic)) {

            ArrayList<Weather> buffer = new ArrayList<>(RECORDS_BUFFER_SIZE);
            int totalRead = 0;
            boolean isTopicEmpty = false;

            while (!isTopicEmpty) {
                List<Weather> records = Collections.emptyList();
                final ConsumerRecords<byte[], Weather> consumerRecords = consumer.poll(KAFKA_POLL_TIMEOUT_MS);
                if (consumerRecords.count() == 0) {
                    LOG.trace("no more messages in the topic, messages read total={}", totalRead);
                    isTopicEmpty = true;
                } else {
                    LOG.trace("read {} records from topic", consumerRecords.count());
//                    if (LOG.isDebugEnabled()) {
//                        consumerRecords.forEach(record -> LOG.debug("key={}, value={}", record.key(), record.value()));
//                    }
                    records = createStream(consumerRecords.iterator())
                            .map(ConsumerRecord::value)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                    totalRead += consumerRecords.count();
                    consumer.commitAsync();
                }
                saveToHdfs(buffer, records, isTopicEmpty, ss, path);
            }
        } finally {
            LOG.debug("final section of try");
        }
        LOG.info("END");
    }

    private static void saveToHdfs(List<Weather> buffer, List<Weather> records,
                                   boolean isTopicEmpty, SparkSession ss, String path) {
        if (isTopicEmpty) {
            LOG.trace("saving rest of the buffer to hdfs, size={}", buffer.size());
            saveToHdfsBySpark(buffer, ss, path);
        } else if (buffer.size() + records.size() >= RECORDS_BUFFER_SIZE) {
            LOG.trace("no free space in buffer, saving its records to hdfs");
            saveToHdfsBySpark(buffer, ss, path);
            buffer.clear();
            buffer.addAll(records);
        } else {
            buffer.addAll(records);
            LOG.trace("buffer is filled in with new records, size now is {}", buffer.size());
        }
    }

    private static void saveToHdfsBySpark(List<Weather> records,
                                          SparkSession ss,
                                          String path) {
        JavaSparkContext sparkContext = new JavaSparkContext(ss.sparkContext());
        JavaRDD<Weather> rdd = sparkContext.parallelize(records);
        Dataset<Row> df = ss.createDataFrame(rdd, Weather.class);
        LOG.trace("appending output with {} records", records.size());
        df.coalesce(1).write().mode(SaveMode.Append).parquet(path);
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
        props.put("max.poll.records", KAFKA_MAX_POLL_RECORDS);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "kafka.serdes.weather.JsonWeatherDeserializer");
//        props.put("enable.auto.commit", "true");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("session.timeout.ms", "30000");

        //not a good idea, see
        //https://stackoverflow.com/questions/28561147/how-to-read-data-using-kafka-consumer-api-from-beginning
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "topic-saver3-id");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

}
