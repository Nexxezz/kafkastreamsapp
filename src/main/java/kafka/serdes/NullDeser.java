package kafka.serdes;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class NullDeser implements Deserializer<String> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public String deserialize(String s, byte[] bytes) {
        return "null";
    }

    @Override
    public void close() {

    }
}
