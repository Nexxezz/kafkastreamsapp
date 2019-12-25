package kafka.serdes.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Hotel;
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
        return hotel.toString().getBytes();
    }

    @Override
    public void close() {
    }

}
