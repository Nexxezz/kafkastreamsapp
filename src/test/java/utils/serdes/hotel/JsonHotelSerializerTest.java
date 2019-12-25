package utils.serdes.hotel;

import kafka.data.Hotel;
import kafka.serdes.hotel.JsonHotelSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


class JsonHotelSerializerTest {

    private static final Hotel testHotel = new Hotel(4L, "Best Western Holiday Hills", "US",
            "Coalville", "500 W 120 S", 40.91089, -111.40339, "9x2b8");

    public static final String testHotelAsString = "4,Best Western Holiday Hills,US,Coalville,500 W 120 S,40.91089,-111.40339,9x2b8";

    @Test
    void serialize() {
        JsonHotelSerializer serializer = new JsonHotelSerializer();

        byte[] actualHotelBytes = serializer.serialize("topic", testHotel);

        assertArrayEquals(testHotelAsString.getBytes(), actualHotelBytes);
    }

    @Test
    void toStringTest() {
        assertEquals(testHotelAsString, testHotel.toString());
    }
}
