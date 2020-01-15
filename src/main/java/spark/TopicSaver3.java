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

                    List<ConsumerRecord<byte[], Weather>> filteredRecords = createStream(consumerRecords.iterator())
                            .filter(r -> r.key() != null)
                            .filter(r -> r.value() != null)
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

    private static void saveToHdfsBySpark(List<ConsumerRecord<byte[], Weather>> records,
                                          SparkSession ss,
                                          String path) {
        JavaSparkContext sparkContext = new JavaSparkContext(ss.sparkContext());

        JavaRDD<ConsumerRecord<byte[], Weather>> rdd = sparkContext.parallelize(records);
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

/*
package com.cloudurable.kafka.consumer;
import com.cloudurable.kafka.StockAppConstants;
import com.cloudurable.kafka.producer.model.StockPrice;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
...

public class SimpleStockPriceConsumer {

    private static Consumer<String, StockPrice> createConsumer() {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StockAppConstants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Custom Deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StockDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        // Create the consumer using props.
        final Consumer<String, StockPrice> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(StockAppConstants.TOPIC));

        return consumer;
    }
    ...
}
 */

/*
package com.cloudurable.kafka.consumer;
...

public class SimpleStockPriceConsumer {
    ...

    static void runConsumer() throws InterruptedException {
        final Consumer<String, StockPrice> consumer = createConsumer();
        final Map<String, StockPrice> map = new HashMap<>();
        try {
            final int giveUp = 1000; int noRecordsCount = 0;
            int readCount = 0;
            while (true) {
                final ConsumerRecords<String, StockPrice> consumerRecords =
                        consumer.poll(1000);
                if (consumerRecords.count() == 0) {
                    noRecordsCount++;
                    if (noRecordsCount > giveUp) break;
                    else continue;
                }
                readCount++;
                consumerRecords.forEach(record -> {
                    map.put(record.key(), record.value());
                });
                if (readCount % 100 == 0) {
                    displayRecordsStatsAndStocks(map, consumerRecords);
                }
                consumer.commitAsync();
            }
        }
        finally {
            consumer.close();
        }
        System.out.println("DONE");
    }
    ...
    public static void main(String... args) throws Exception {
      runConsumer();
    }
    ...
}

 */