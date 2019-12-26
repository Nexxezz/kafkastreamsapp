package kafka.app;

import kafka.data.Hotel;
import kafka.serdes.hotel.JsonHotelDeserializer;
import kafka.serdes.hotel.JsonHotelSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class Joiner {


    public static void main(String[] args) {

        final Serde<Hotel> hotelSerde = Serdes.serdeFrom(new JsonHotelSerializer(), new JsonHotelDeserializer());


        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Hotel> hotelsDictionary = builder.stream("hotels-dictionary-topic", Consumed.with(Serdes.String(), hotelSerde));

    }
}
