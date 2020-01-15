package kafka.apidemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class ConsumerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerExample.class);
    public final static String DEFAULT_BOOTSTRAP_SERVER = "sandbox-hdp.hortonworks.com:2181";
    public final static String DEFAULT_TOPIC_NAME = "streamsets-hotels-topic";

    public static void main(String[] args) {
        String bootstrapServer;
        String topic;
        if (args.length == 0) {
            bootstrapServer = DEFAULT_BOOTSTRAP_SERVER;
            topic = DEFAULT_TOPIC_NAME;
        } else {
            bootstrapServer = args[0];
            topic = args[1];
        }
        LOG.debug("Initial parameters:\n" + "TOPIC_NAME: " + topic + "  BOOTSTRAP_SERVER: " + bootstrapServer);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            System.out.println("retrieving records");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("record reading..");
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            }
        }
    }
}
