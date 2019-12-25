package kafka.app;

import kafka.serdes.NullDeser;
import kafka.serdes.NullSer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class EvgeniiApp {


    public static void main(String[] args) {
        System.out.println("START");

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:6667");
//        streamsConfiguration.put("value.serializer", "kafka.serdes.NullSer");
//        streamsConfiguration.put("value.deserializer", "kafka.serdes.NullDeser");

        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

//        Serde<String> stringSerde = Serdes.String();
//        Serde<String> nullSerde = Serdes.serdeFrom(new NullSer(), new NullDeser());
        final Serde<String> stringSerde = Serdes.String();
        final Serde<byte[]> byteArraySerde = Serdes.ByteArray();


        StreamsBuilder builder = new StreamsBuilder();
        KStream<byte[], String> weatherStream = builder.stream("weather-topic", Consumed.with(byteArraySerde, stringSerde));
//        KStream<String, String> weatherStream = builder.stream("weather-topic");
        weatherStream.to("SOME_TOPIC");

        //weatherStream.foreach((key, value) -> System.out.println("key=" + key + ", value=" + value));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.out.println("END");
    }
}
