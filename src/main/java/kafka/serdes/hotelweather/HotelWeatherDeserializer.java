package kafka.serdes.hotelweather;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.HotelWeather;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class HotelWeatherDeserializer implements Deserializer<HotelWeather> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public HotelWeather deserialize(String s, byte[] bytes) {
        if (bytes == null)
            return new HotelWeather();
        try {
            return mapper.treeToValue(mapper.readTree(bytes), HotelWeather.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new HotelWeather();

    }

    @Override
    public void close() {

    }
}
