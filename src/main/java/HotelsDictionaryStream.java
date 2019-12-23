import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class HotelsDictionaryStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hotels-dictionary-topic-get");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667");

        StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> source = builder.stream("hotels-topic", Consumed.with(Serdes.String(),Serdes.String()));
                source.mapValues(msg->{
                    String[]  values = msg.split(",");
                    List<KeyValue<String,String>> dictionaryList = new LinkedList<>();
                    dictionaryList.add(KeyValue.pair("id",values[0]));
                    dictionaryList.add(KeyValue.pair("name",values[1]));
                    dictionaryList.add(KeyValue.pair("country",values[2]));
                    dictionaryList.add(KeyValue.pair("city",values[3]));
                    dictionaryList.add(KeyValue.pair("address",values[4]));
                    dictionaryList.add(KeyValue.pair("latitude",values[5]));
                    dictionaryList.add(KeyValue.pair("longitude",values[6]));
                    dictionaryList.add(KeyValue.pair("geohash",values[7]));
                    return dictionaryList.toString();
                }).to("hotels-dictionary-topic-data", Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (
                Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
