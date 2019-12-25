package utils.serdes.hotel;

import kafka.data.Hotel;
import kafka.serdes.hotel.JsonHotelDeserializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonHotelDeserializerTest {

    @Test
    void deserialize() {
        JsonHotelDeserializer deserializer = new JsonHotelDeserializer();
        String[] message = "3427383902209,H tel Barri re Le Fouquet s,FR,Paris,46 Avenue George V 8th arr 75008 Paris France,48.8710709,2.3013119,u09wh".split(",");
        Hotel actualHotel = deserializer.deserialize("topic","3427383902209,H tel Barri re Le Fouquet s,FR,Paris,46 Avenue George V 8th arr 75008 Paris France,48.8710709,2.3013119,u09wh".getBytes());
        Hotel expectedHotel = new Hotel(Long.parseLong(message[0]),message[1],message[2],message[3],message[4],message[5],message[6],message[7]);
        assertEquals(expectedHotel,actualHotel);
    }
}