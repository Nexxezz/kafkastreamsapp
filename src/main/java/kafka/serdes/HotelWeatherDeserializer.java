package kafka.serdes;

import kafka.data.HotelWeather;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class HotelWeatherDeserializer implements Deserializer<HotelWeather> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /***
     * Method for HotelWeather object deserialization
     * @param topic  Kafka topic name
     * @param data  serialized HotelWeather class for deserialization
     * @return HotelWeather object
     */
    @Override
    public HotelWeather deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        return HotelWeather.ofString(new String(data));
    }

    @Override
    public void close() {

    }
}
