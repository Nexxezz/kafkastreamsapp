package utils.serdes.hotel;

import kafka.data.Hotel;
import kafka.serdes.hotel.JsonHotelSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


class JsonHotelSerializerTest {

    private static final Hotel testHotel = new Hotel(23L, "name", "country",
            "city", "address", 5.0, 6.0, "geoHash");

    public static final String testHotelAsString = "23,name,country,city,address,5.0,6.0,geoHash";

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
