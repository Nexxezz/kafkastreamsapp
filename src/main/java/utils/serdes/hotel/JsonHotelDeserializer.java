package utils.serdes.hotel;

import com.fasterxml.jackson.databind.ObjectMapper;
import data.Hotel;
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
        if (bytes == null) throw new RuntimeException("deserialize received null bytes");
        String[] arr = new String(bytes).split(",");
        return new Hotel(Integer.parseInt(arr[0]), arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7]);
    }

//        @Override
//        public Hotel deserialize(String topic, byte[] bytes) {
//            if (bytes == null)
//                return null;
//
//            try {
//
//                byte[] message = new String(bytes).split(",").toString().getBytes();
//                System.out.println(objectMapper.treeToValue(objectMapper.readTree(message), Hotel.class));
//                return objectMapper.treeToValue(objectMapper.readTree(bytes), Hotel.class);
//            } catch (Exception e) {
//                throw new SerializationException(e);
//            }
//
//        }

    @Override
    public void close() {

    }
}
