package kafka.serdes;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class NullSer implements Serializer<String> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, String s2) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
