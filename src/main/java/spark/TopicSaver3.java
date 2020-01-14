package spark;

import kafka.data.HotelWeather;
import kafka.serdes.HotelWeatherDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class TopicSaver3 {

    public final static Logger LOG = LoggerFactory.getLogger(TopicSaver3.class.getName());
    public static final String TOPIC_NAME = "join-result-topic";
    public static final String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/topicSaverResult";
    public static final int RECORDS_LIMIT = 1000;
    final static Deserializer<HotelWeather> hotelWeatherDeserializer = new HotelWeatherDeserializer();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("group.id", UUID.randomUUID().toString());
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", HotelWeatherDeserializer.class.getName());

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
        KafkaConsumer<String, HotelWeather> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(topic));
        for (int i = 0; i < limit; i++) {
            ConsumerRecords<String, HotelWeather> records = consumer.poll(100);
            for (ConsumerRecord<String, HotelWeather> record : records)
                System.out.println("key: " + record.key() + " " + "value: " + record.value());
        }
    }

}