package com.example.allenstreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public final class TaggedIntervalSerde {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Serde<TaggedInterval> serde() {
        return Serdes.serdeFrom(new Ser(), new De());
    }

    private static final class Ser implements Serializer<TaggedInterval> {
        @Override
        public byte[] serialize(String topic, TaggedInterval data) {
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

    private static final class De implements Deserializer<TaggedInterval> {
        @Override
        public TaggedInterval deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.readValue(data, TaggedInterval.class);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    private TaggedIntervalSerde() {}
}
