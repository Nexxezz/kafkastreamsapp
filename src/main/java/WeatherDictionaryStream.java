import ch.hsr.geohash.GeoHash;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import serdes.weather.JsonWeatherDeserializer;
import serdes.weather.JsonWeatherSerializer;

import javax.ws.rs.DefaultValue;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class WeatherDictionaryStream {
    static class WeatherMessage {

        private Double lng;
        private Double lat;
        private Double avg_tmpr_f;
        private Double avg_tmpr_c;
        private String wthr_date;

        @DefaultValue("empty")
        private String geoHash;

        public Double getLng() {
            return lng;
        }

        public void setLng(Double lng) {
            this.lng = lng;
        }

        public Double getLat() {
            return lat;
        }

        public void setLat(Double lat) {
            this.lat = lat;
        }

        public Double getAvg_tmpr_f() {
            return avg_tmpr_f;
        }

        public void setAvg_tmpr_f(Double avg_tmpr_f) {
            this.avg_tmpr_f = avg_tmpr_f;
        }

        public Double getAvg_tmpr_c() {
            return avg_tmpr_c;
        }

        public void setAvg_tmpr_c(Double avg_tmpr_c) {
            this.avg_tmpr_c = avg_tmpr_c;
        }

        public String getWthr_date() {
            return wthr_date;
        }

        public void setWthr_date(String wthr_date) {
            this.wthr_date = wthr_date;
        }

        public void setGeoHash(String geoHash) {
            this.geoHash = geoHash;
        }

        public String getGeoHash() {
            return geoHash;
        }
    }


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-dictionary-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");

        Map<String, Object> serdeProps = new HashMap<>();

        final Serializer<WeatherMessage> weatherMessageSerializer = new JsonWeatherSerializer<>();
        serdeProps.put("JsonPOJOClass", WeatherMessage.class);
        weatherMessageSerializer.configure(serdeProps, false);

        final Deserializer<WeatherMessage> weatherMessageDeserializer = new JsonWeatherDeserializer<>();
        serdeProps.put("JsonPOJOClass", WeatherMessage.class);
        weatherMessageDeserializer.configure(serdeProps, false);

        final Serde<WeatherMessage> weatherMessageSerde = Serdes.serdeFrom(weatherMessageSerializer, weatherMessageDeserializer);


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, WeatherMessage> weatherInput = builder.stream("weather-topic", Consumed.with(Serdes.String(), weatherMessageSerde));
        weatherInput.mapValues(msg -> {
            msg.setGeoHash(GeoHash.geoHashStringWithCharacterPrecision(msg.getLat(), msg.getLng(), 5));
            return msg;
        }).to("weather-dictionary-topic", Produced.with(Serdes.String(), weatherMessageSerde));
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        System.out.println(topology.describe());
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

