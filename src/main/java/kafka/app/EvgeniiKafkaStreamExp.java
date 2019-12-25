package kafka.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class EvgeniiKafkaStreamExp {

    public static final Logger LOG = LoggerFactory.getLogger(EvgeniiKafkaStreamExp.class);

    public static void main(String[] args) {
        String topic = args[1];
        LOG.info("START with arg={}", topic);

        Properties props = getProperties();


    }

    private static Properties getProperties() {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "evgenii-kafka-stream-exp");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "evgenii-kafka-stream-exp-client");

        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        //streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return null;
    }
}
