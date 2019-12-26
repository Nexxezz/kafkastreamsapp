package kafka.serdes.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Weather;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonWeatherSerializer implements Serializer<Weather> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonWeatherSerializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    /***
     * Method for Weather object serialization
     * @param topic  Kafka topic name
     * @param weather  Weather object for serialization
     * @return byte array that represents Weather object
     */
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
