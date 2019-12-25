package kafka.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class EvgeniiExp {

    public static void main(String[] args) {
        String topicName = args[1];
        KafkaConsumer<String, String> consumer = createAndSubscribe(topicName);

        //...
    }

    public static KafkaConsumer<String, String> createAndSubscribe(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "evgenii-kafka-app");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topicName));
        return consumer;
    }
}
