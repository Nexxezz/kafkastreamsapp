package kafka.serdes;

import kafka.data.HotelWeather;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HotelWeatherDeserializerTest {

    private static HotelWeather hotelWeather = new HotelWeather(4L, "Best Western Holiday Hills",
            40.91089, -111.40339, "date");

    private static String hotelWeatherAsString = "4,Best Western Holiday Hills,40.91089,-111.40339,date";

    @Test
    void deserialize() {
        HotelWeatherDeserializer deserializer = new HotelWeatherDeserializer();
        HotelWeather actualHotelWeather = deserializer.deserialize("topic", hotelWeatherAsString.getBytes());
        assertEquals(hotelWeather, actualHotelWeather);
    }
}