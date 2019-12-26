package kafka.serdes.weather;

import kafka.data.Weather;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonWeatherDeserializerTest {

    private String json = "{\"lng\":-111.012,\"lat\":18.7711,\"avg_tmpr_f\":82.5,\"avg_tmpr_c\":28.1,\"wthr_date\":\"2016-10-12\"}";
    private Weather weather = new Weather(-111.012, 18.7711, 82.5, 28.1, "2016-10-12", null);

    @Test
    void deserialize() {
        JsonWeatherDeserializer deserializer = new JsonWeatherDeserializer();
        Weather actualWeather = deserializer.deserialize("topic", json.getBytes());
        assertEquals(weather, actualWeather);
    }
}