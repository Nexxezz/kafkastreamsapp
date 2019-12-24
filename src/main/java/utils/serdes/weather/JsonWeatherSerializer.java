package utils.serdes.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import java.util.Map;
import data.Weather;
import org.apache.kafka.common.serialization.Serializer;

public class JsonWeatherSerializer implements Serializer<Weather> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonWeatherSerializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Weather weather) {
        if (weather == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(weather);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}
