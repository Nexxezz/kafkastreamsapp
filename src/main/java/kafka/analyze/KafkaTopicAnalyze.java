package kafka.analyze;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaTopicAnalyze {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaTopicAnalyze.class);

    public static final int LOG_LINES_LIMIT = 10;

    public static void main(String[] args) {
        String topic = args[0];
        LOG.info("STARTED for topic={}, first {} messages will be picked up", topic);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<byte[], byte[]> weather = builder.stream(topic);

        AtomicInteger logged = new AtomicInteger(0);

        weather.map((key, value) -> {
            String keyStr;
            if (key == null) keyStr = "NULL-KEY";
            else if (key.length == 0) keyStr = "EMPTY-KEY";
            else keyStr = new String(key);

            String valueStr;
            if (value == null) valueStr = "NULL-VALUE";
            else if (value.length == 0) valueStr = "EMPTY-VALUE";
            else valueStr = new String(value);

            if (logged.incrementAndGet() <= LOG_LINES_LIMIT)
                LOG.debug("key={} value={}", keyStr, valueStr);

            return KeyValue.pair(key, value);
        });

        weather.to("SOME_OTHER_TOPIC");
        proceedStreams(builder);
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
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return streamsConfiguration;
    }
}
