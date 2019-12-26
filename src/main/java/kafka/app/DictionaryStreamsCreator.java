package kafka.app;

import ch.hsr.geohash.GeoHash;
import kafka.data.Hotel;
import kafka.data.HotelWeather;
import kafka.data.Weather;
import kafka.serdes.hotel.JsonHotelDeserializer;
import kafka.serdes.hotel.JsonHotelSerializer;
import kafka.serdes.hotelweather.HotelWeatherDeserializer;
import kafka.serdes.hotelweather.HotelWeatherSerializer;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class DictionaryStreamsCreator {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");

        final Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<Weather> weatherSerializer = new JsonWeatherSerializer();
        serdeProps.put("weatherSer", Weather.class);

        final Deserializer<Weather> weatherDeserializer = new JsonWeatherDeserializer();
        serdeProps.put("weatherDe", Weather.class);

        final Serializer<Hotel> hotelSerializer = new JsonHotelSerializer();
        serdeProps.put("hotelSer", Hotel.class);

        final Deserializer<Hotel> hotelDeserializer = new JsonHotelDeserializer();
        serdeProps.put("hotelDe", Hotel.class);


        final Serializer<HotelWeather> hotelWeatherSerializer = new HotelWeatherSerializer();
        serdeProps.put("hotelWeatherSer", HotelWeather.class);

        final Deserializer<HotelWeather> hotelWeatherDeserializer = new HotelWeatherDeserializer();
        serdeProps.put("hotelWeatherDe", HotelWeather.class);

        final Serde<Weather> weatherSerde = Serdes.serdeFrom(weatherSerializer, weatherDeserializer);
        final Serde<Hotel> hotelSerde = Serdes.serdeFrom(hotelSerializer, hotelDeserializer);
        final Serde<HotelWeather> hotelWeatherSerde = Serdes.serdeFrom(hotelWeatherSerializer, hotelWeatherDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Weather> weatherInput = builder
                .stream("weather-topic", Consumed.with(Serdes.String(), weatherSerde))
                .mapValues(value -> {
                    value.setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(value.getLat(), value.getLng(), 5));
                    return value;
                })
                .map((key, value) -> KeyValue.pair(value.getGeoHash(), value));

        KStream<String, Hotel> hotelInput = builder
                .stream("hotels-topic", Consumed.with(Serdes.String(), hotelSerde))
                .map((key, value) -> KeyValue.pair(value.getGeoHash(), value));
        KStream<String, HotelWeather> hotelsWeather = hotelInput.join(weatherInput, ((hotel, weather) ->
                        new HotelWeather(hotel.getId(), hotel.getName(), weather.getAverageTemperatureCelsius(),
                                weather.getAverageTemperatureFahrenheit(), weather.getDate())),
                JoinWindows.of(TimeUnit.MINUTES.toMillis(1)),
                Joined.with(Serdes.String(),
                        hotelSerde,
                        weatherSerde));
        hotelsWeather.to("hotel-weather-topic",Produced.with(Serdes.String(),hotelWeatherSerde));

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

