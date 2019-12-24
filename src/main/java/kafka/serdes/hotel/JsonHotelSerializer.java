package kafka.serdes.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Hotel;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
public class JsonHotelSerializer implements Serializer<Hotel> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonHotelSerializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Hotel hotel) {
        if (hotel == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(hotel);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}
