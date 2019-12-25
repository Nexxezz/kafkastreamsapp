package utils.serdes.hotel;

import kafka.data.Hotel;
import kafka.serdes.hotel.JsonHotelDeserializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonHotelDeserializerTest {

    @Test
    void deserialize() {
        JsonHotelDeserializer deserializer = new JsonHotelDeserializer();

        byte[] bytes = "4,Best Western Holiday Hills,US,Coalville,500 W 120 S,40.91089,-111.40339,9x2b8\n\r".getBytes();
        Hotel actualHotel = deserializer.deserialize("topic", bytes);

        Hotel expectedHotel = new Hotel(4L, "Best Western Holiday Hills", "US",
                "Coalville", "500 W 120 S", 40.91089, -111.40339, "9x2b8");

        assertEquals(expectedHotel, actualHotel);
    }
}