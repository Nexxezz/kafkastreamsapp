package spark;

import kafka.data.Hotel;
import kafka.data.HotelWeather;
import kafka.data.Weather;
import kafka.serdes.HotelWeatherDeserializer;
import kafka.serdes.HotelWeatherSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class TopicSaver3 {

    public final static Logger LOG = LoggerFactory.getLogger(TopicSaver3.class.getName());
    public static final String TOPIC_NAME = "join-result-topic";
    public static final String HDFS_PATH = "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/topicSaverResult";
    public static final int RECORDS_LIMIT = 50;
    final static Serializer<HotelWeather> hotelWeatherSerializer = new HotelWeatherSerializer();
    final static  Deserializer<HotelWeather> hotelWeatherDeserializer = new HotelWeatherDeserializer();
    final static Serde<HotelWeather> hotelWeatherSerde = Serdes.serdeFrom(hotelWeatherSerializer, hotelWeatherDeserializer);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");

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
        ConsumerRecords<String, HotelWeather> records = consumer.poll(100);
        for (ConsumerRecord<String, HotelWeather> record : records)
            System.out.println("key: " + record.key() + " " + "value: " + record.value());

    }

}