package kafka.serdes;

import kafka.data.HotelWeather;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class HotelWeatherSerializer implements Serializer<HotelWeather> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    /***
     * Method for HotelWeather object serialization
     * @param topic Kafka topic name
     * @param hotelWeather HotelWeather object for serialization
     * @return byte array that represents HotelWeather object
     */
    @Override
    public byte[] serialize(String topic, HotelWeather hotelWeather) {
        return hotelWeather.toString().getBytes();
    }

    @Override
    public void close() {

    }
}
