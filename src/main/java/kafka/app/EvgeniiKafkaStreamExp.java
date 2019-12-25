package kafka.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class EvgeniiKafkaStreamExp {

    public static final Logger LOG = LoggerFactory.getLogger(EvgeniiKafkaStreamExp.class);

    public static void main(String[] args) {
        LOG.info("START");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> weather = builder.stream("weather-topic");
//        weather.map()
        weather.to("SOME_OTHER_TOPIC");


        proceedStreams(builder);

        LOG.info("END");
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
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "evgenii-kafka-stream-exp");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return streamsConfiguration;
    }
}
