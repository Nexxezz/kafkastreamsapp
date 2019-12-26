package kafka.app;

import kafka.data.Hotel;
import kafka.data.HotelWeather;
import kafka.data.Weather;
import kafka.serdes.HotelWeatherDeserializer;
import kafka.serdes.HotelWeatherSerializer;
import kafka.serdes.hotel.JsonHotelDeserializer;
import kafka.serdes.hotel.JsonHotelSerializer;
import kafka.serdes.weather.JsonWeatherDeserializer;
import kafka.serdes.weather.JsonWeatherSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static ch.hsr.geohash.GeoHash.geoHashStringWithCharacterPrecision;

/***
 *  Class  for creating Hotels and Weather streams from Kafka topics.
 *  Third stream created as the result of join operation on geohash between Hotels and Weather streams.
 *  All classes uses custom value serde.
 */
public class DictionaryStreamsCreator {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");

        final Serializer<Weather> weatherSerializer = new JsonWeatherSerializer();
        final Deserializer<Weather> weatherDeserializer = new JsonWeatherDeserializer();

        final Serializer<Hotel> hotelSerializer = new JsonHotelSerializer();
        final Deserializer<Hotel> hotelDeserializer = new JsonHotelDeserializer();

        final Serializer<HotelWeather> hotelWeatherSerializer = new HotelWeatherSerializer();
        final Deserializer<HotelWeather> hotelWeatherDeserializer = new HotelWeatherDeserializer();

        final Serde<Weather> weatherSerde = Serdes.serdeFrom(weatherSerializer, weatherDeserializer);
        final Serde<Hotel> hotelSerde = Serdes.serdeFrom(hotelSerializer, hotelDeserializer);

        final Serde<HotelWeather> hotelWeatherSerde = Serdes.serdeFrom(hotelWeatherSerializer, hotelWeatherDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        /*KStream<String, Weather> weatherWithKey = */
        builder
                .stream("weather-topic", Consumed.with(Serdes.String(), weatherSerde))
                .mapValues(weather -> {
                    weather.setGeoHash(geoHashStringWithCharacterPrecision(weather.getLat(), weather.getLng(), 5));
                    return weather;
                }).to("weather-dictionary-topic", Produced.with(Serdes.String(), weatherSerde));


        KStream<String, Weather> weatherWithKey = builder
                .stream("weather-dictionary-topic", Consumed.with(Serdes.String(), weatherSerde))
                .map((key, value) -> KeyValue.pair(value.getGeoHash(), value));

        KStream<String, Hotel> hotelsWithKey = builder
                .stream("hotels-data", Consumed.with(Serdes.String(), hotelSerde))
                .filterNot(((key, value) -> value == null))
                .filterNot(((key, value) -> value.getGeoHash() == null))
                .map((key, value) -> KeyValue.pair(value.getGeoHash(), value));

        KStream<String, HotelWeather> hotelWeatherStream = hotelsWithKey.join(weatherWithKey,
                (leftHotel, rightWeather) ->
                        new HotelWeather(leftHotel.getId(), leftHotel.getName(),
                                rightWeather.getAverageTemperatureFahrenheit(),
                                rightWeather.getAverageTemperatureCelsius(),
                                rightWeather.getDate()),
                JoinWindows.of(TimeUnit.MINUTES.toMillis(1)),
                Joined.with(Serdes.String(), hotelSerde, weatherSerde));

        hotelWeatherStream.to("FINAL_TOPIC", Produced.with(Serdes.String(), hotelWeatherSerde));


        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-hook") {
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

//bin/kafka-topics.sh --zookeeper sandbox-hdp.hortonworks.com:2181 --delete --topic