package kafka.serdes.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Hotel;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class JsonHotelDeserializer implements Deserializer<Hotel> {

    private ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(JsonHotelSerializer.class);

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
            String[] message = new String(bytes).split(",");

            Hotel hotel = new Hotel(Long.parseLong(message[0]), message[1], message[2], message[3], message[4],
                    message[5], message[6], message[7].substring(0,message[7].length()-2));
            return hotel;
        } catch (Exception e) {
            throw new SerializationException(e);
        }

    }

    @Override
    public void close() {

    }
}
