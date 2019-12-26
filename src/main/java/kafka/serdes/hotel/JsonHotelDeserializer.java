package kafka.serdes.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.data.Hotel;
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

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    }

    @Override
    public Hotel deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        String[] arr = new String(bytes).split(",");

        if (arr.length != 8)
            return new Hotel();
        else {
            String geoHash = (arr[7].matches(".+\\n\\r")) ? arr[7].substring(0, arr[7].length() - 2) : arr[7];
            return new Hotel(Long.parseLong(arr[0]), arr[1], arr[2], arr[3], arr[4],
                    Double.valueOf(arr[5]), Double.valueOf(arr[6]), geoHash);
        }
    }

    @Override
    public void close() {

    }
}
