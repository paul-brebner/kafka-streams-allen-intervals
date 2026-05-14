package com.example.allenstreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public final class IntervalBuffersSerde {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Serde<IntervalBuffers> serde() {
        return Serdes.serdeFrom(new Ser(), new De());
    }

    private static final class Ser implements Serializer<IntervalBuffers> {
        @Override
        public byte[] serialize(String topic, IntervalBuffers data) {
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

    private static final class De implements Deserializer<IntervalBuffers> {
        @Override
        public IntervalBuffers deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.readValue(data, IntervalBuffers.class);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    private IntervalBuffersSerde() {}
}
