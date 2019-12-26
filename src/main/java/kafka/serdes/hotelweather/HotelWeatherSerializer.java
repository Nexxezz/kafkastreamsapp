package kafka.serdes.hotelweather;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.HotelWeather;
import kafka.data.Weather;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class HotelWeatherSerializer implements Serializer<HotelWeather> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, HotelWeather hotelWeather) {
        if (hotelWeather == null)
            return null;

        try {
            return mapper.writeValueAsBytes(hotelWeather);
        } catch (Exception e) {
            throw new SerializationException("Error serializing message", e);
        }
    }

    @Override
    public void close() {

    }
}
