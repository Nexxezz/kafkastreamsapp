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

    /***
     * Method for Hotel object serialization
     * @param topic  Kafka topic name
     * @param hotel  Hotel object for serialization
     * @return byte array that represents Hotel object
     */
    @Override
    public byte[] serialize(String topic, Hotel hotel) {
        return hotel.toString().getBytes();
    }

    @Override
    public void close() {
    }

}
