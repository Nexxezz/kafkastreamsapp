package kafka.serdes.weather;

import kafka.data.Weather;

class JsonWeatherSerializerTest {

    private String json = "{\"lng\":-111.012,\"lat\":18.7711,\"avg_tmpr_f\":82.5,\"avg_tmpr_c\":28.1,\"wthr_date\":\"2016-10-12\",\"geoHash\":\"geoHash\"}";
    private Weather weather = new Weather(-111.012, 18.7711, 82.5, 28.1, "2016-10-12", "geoHash");


//    @Test
//    void serialize() {
//        JsonWeatherSerializer serializer = new JsonWeatherSerializer();
//        byte[] bytes = serializer.serialize("topic", weather);
//        assertArrayEquals(json.getBytes(), bytes);
//    }
}