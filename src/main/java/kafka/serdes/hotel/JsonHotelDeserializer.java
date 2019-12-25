package kafka.serdes.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Hotel;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonHotelDeserializer implements Deserializer<Hotel> {

    private ObjectMapper objectMapper = new ObjectMapper();


    /**
     * Default constructor needed by Kafka
     */
    public JsonHotelDeserializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public Hotel deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            byte[] message = new String(bytes).split(",").toString().getBytes();
            return objectMapper.treeToValue(objectMapper.readTree(message), Hotel.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }

    }

    @Override
    public void close() {

    }
}
