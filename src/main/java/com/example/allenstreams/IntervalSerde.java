package com.example.allenstreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public final class IntervalSerde {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Serde<IntervalRecord> serde() {
        return Serdes.serdeFrom(new Ser(), new De());
    }

    private static final class Ser implements Serializer<IntervalRecord> {
        @Override
        public byte[] serialize(String topic, IntervalRecord data) {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    private static final class De implements Deserializer<IntervalRecord> {
        @Override
        public IntervalRecord deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.readValue(data, IntervalRecord.class);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    /** JSON map for sink topics / logs without coupling consumers to this JAR. */
    public static byte[] inferenceToJsonBytes(String key, AllenRelation r, IntervalRecord a, IntervalRecord b)
            throws Exception {
        Map<String, Object> m =
                Map.of(
                        "key",
                        key,
                        "relation",
                        r.name(),
                        "a",
                        Map.of("startMs", a.startMs(), "endMs", a.endMs()),
                        "b",
                        Map.of("startMs", b.startMs(), "endMs", b.endMs()));
        return MAPPER.writeValueAsBytes(m);
    }

    private IntervalSerde() {}
}
