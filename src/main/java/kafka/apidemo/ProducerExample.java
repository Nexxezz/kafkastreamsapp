package kafka.apidemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * class example for testing Kafka Producer API with Hortonworks Sandbox HDP
 */
public class ProducerExample {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerExample.class);
    public final static String DEFAULT_BOOTSTRAP_SERVER = "sandbox-hdp.hortonworks.com:6667";
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
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 50; i++) {
            LOG.debug("Started writing messages to topic: " + topic);
            LOG.debug("key: "+ i);
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), UUID.randomUUID().toString()));
            System.out.println("Message sent successfully");
        }
        producer.close();
    }
}
