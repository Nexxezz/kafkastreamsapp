/*package utils.serdes.hotel;

import kafka.data.Hotel;
import kafka.serdes.hotel.JsonHotelSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class JsonHotelSerializerTest {

    @Test
    void serialize() {
        JsonHotelSerializer serializer = new JsonHotelSerializer();
        Hotel hotel = new Hotel(23L, "1", "2", "3", "4", "5", "6", "7");
        byte[] expectedHotelBytes = hotel.toString().getBytes();
        System.out.println(hotel.toString());
        byte[] actualHotelBytes = serializer.serialize("topic",hotel);

        assertEquals(expectedHotelBytes, actualHotelBytes);
    }
}
*/