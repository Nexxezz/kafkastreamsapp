package kafka.serdes;

import kafka.data.HotelWeather;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class HotelWeatherDeserializer implements Deserializer<HotelWeather> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public HotelWeather deserialize(String topic, byte[] data) {
        return HotelWeather.ofString(new String(data));
    }

    @Override
    public void close() {

    }
}
