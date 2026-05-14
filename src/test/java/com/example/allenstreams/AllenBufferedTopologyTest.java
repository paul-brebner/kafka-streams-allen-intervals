package com.example.allenstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AllenBufferedTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TopologyTestDriver driver;

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
            driver = null;
        }
    }

    @Test
    void bufferedOverlappingIntervalsFarApartStarts_emitsOverlaps() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        AllenBufferedTopology.build(builder);

        Properties cfg = new Properties();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "allen-buffer-test");
        cfg.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        driver = new TopologyTestDriver(builder.build(), cfg);

        IntervalRecord a = new IntervalRecord(0, 1_000_000);
        IntervalRecord b = new IntervalRecord(999_000, 1_000_001);

        TestInputTopic<String, IntervalRecord> inA =
                driver.createInputTopic(
                        AllenInferenceTopology.TOPIC_A,
                        new StringSerializer(),
                        IntervalSerde.serde().serializer());
        TestInputTopic<String, IntervalRecord> inB =
                driver.createInputTopic(
                        AllenInferenceTopology.TOPIC_B,
                        new StringSerializer(),
                        IntervalSerde.serde().serializer());
        TestOutputTopic<String, byte[]> out =
                driver.createOutputTopic(
                        AllenBufferedTopology.TOPIC_OUT,
                        new StringDeserializer(),
                        new ByteArrayDeserializer());

        inA.pipeInput("k", a, Instant.ofEpochMilli(a.startMs()));
        inB.pipeInput("k", b, Instant.ofEpochMilli(b.startMs()));

        List<JsonNode> rows = new ArrayList<>();
        for (var kv : out.readKeyValuesToList()) {
            rows.add(MAPPER.readTree(kv.value));
        }

        assertEquals(1, rows.size(), () -> rows.toString());
        assertEquals("OVERLAPS", rows.get(0).path("relation").asText());
    }

    @Test
    void bufferedOrderBThenA_stillEmitsOverlaps() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        AllenBufferedTopology.build(builder);

        Properties cfg = new Properties();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "allen-buffer-test-2");
        cfg.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        driver = new TopologyTestDriver(builder.build(), cfg);

        IntervalRecord a = new IntervalRecord(0, 1_000_000);
        IntervalRecord b = new IntervalRecord(999_000, 1_000_001);

        TestInputTopic<String, IntervalRecord> inA =
                driver.createInputTopic(
                        AllenInferenceTopology.TOPIC_A,
                        new StringSerializer(),
                        IntervalSerde.serde().serializer());
        TestInputTopic<String, IntervalRecord> inB =
                driver.createInputTopic(
                        AllenInferenceTopology.TOPIC_B,
                        new StringSerializer(),
                        IntervalSerde.serde().serializer());
        TestOutputTopic<String, byte[]> out =
                driver.createOutputTopic(
                        AllenBufferedTopology.TOPIC_OUT,
                        new StringDeserializer(),
                        new ByteArrayDeserializer());

        inB.pipeInput("k", b, Instant.ofEpochMilli(b.startMs()));
        inA.pipeInput("k", a, Instant.ofEpochMilli(a.startMs()));

        List<JsonNode> rows = new ArrayList<>();
        for (var kv : out.readKeyValuesToList()) {
            rows.add(MAPPER.readTree(kv.value));
        }

        assertEquals(1, rows.size(), () -> rows.toString());
        assertEquals("OVERLAPS", rows.get(0).path("relation").asText());
    }
}
