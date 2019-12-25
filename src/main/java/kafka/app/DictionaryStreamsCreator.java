package kafka.app;

import ch.hsr.geohash.GeoHash;
import kafka.data.Hotel;
import kafka.data.Weather;
import kafka.serdes.hotel.JsonHotelDeserializer;
import kafka.serdes.hotel.JsonHotelSerializer;
import kafka.serdes.weather.JsonWeatherDeserializer;
import kafka.serdes.weather.JsonWeatherSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class DictionaryStreamsCreator {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");

        final Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Weather> weatherSerializer = new JsonWeatherSerializer();
        serdeProps.put("weatherSer", Weather.class);
        weatherSerializer.configure(serdeProps, false);

        final Deserializer<Weather> weatherDeserializer = new JsonWeatherDeserializer();
        serdeProps.put("weatherDe", Weather.class);
        weatherDeserializer.configure(serdeProps, false);

        final Serializer<Hotel> hotelSerializer = new JsonHotelSerializer();
        serdeProps.put("hotelSer", Hotel.class);
        hotelSerializer.configure(serdeProps, false);

        final Deserializer<Hotel> hotelDeserializer = new JsonHotelDeserializer();
        serdeProps.put("hotelDe", Hotel.class);
        hotelDeserializer.configure(serdeProps, false);

        final Serde<Weather> weatherSerde = Serdes.serdeFrom(weatherSerializer, weatherDeserializer);
        final Serde<Hotel> hotelSerde = Serdes.serdeFrom(hotelSerializer, hotelDeserializer);


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Weather> weatherInput = builder.stream("weather-topic", Consumed.with(Serdes.String(), weatherSerde));
        weatherInput.mapValues(msg -> {
            msg.setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(msg.getLat(), msg.getLng(), 5));
            return msg;
        }).to("weather-dictionary-topic", Produced.with(Serdes.String(), weatherSerde));

        KStream<String, Hotel> source = builder.stream("hotels-topic", Consumed.with(Serdes.String(), hotelSerde));
        source.to("hotels-dictionary-topic", Produced.with(Serdes.String(), hotelSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-hook-thread") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (
                Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

