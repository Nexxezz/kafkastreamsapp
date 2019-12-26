package kafka.serdes.weather;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Weather;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonWeatherDeserializer implements Deserializer<Weather> {
    
    private ObjectMapper objectMapper = new ObjectMapper();


    public JsonWeatherDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {

    }

    /***
     * Method for Weather object deserialization
     * @param topic  Kafka topic name
     * @param bytes  serialized Weather class for deserialization
     * @return Weather object
     */
    @Override
    public Weather deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            return objectMapper.treeToValue(objectMapper.readTree(bytes), Weather.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

    }

    @Override
    public void close() {

    }
}