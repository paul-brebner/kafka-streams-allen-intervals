package com.example.allenstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AllenInferenceTopologyTest {

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
    void joinOverlappingIntervals_emitsOverlapsRelation() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        AllenInferenceTopology.build(builder);
        Topology topology = builder.build();

        Properties cfg = new Properties();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "allen-join-test");
        cfg.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        driver = new TopologyTestDriver(topology, cfg);

        IntervalRecord a = new IntervalRecord(100, 200);
        IntervalRecord b = new IntervalRecord(150, 250);

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
                        AllenInferenceTopology.TOPIC_OUT,
                        new StringDeserializer(),
                        new ByteArrayDeserializer());

        long ta = a.startMs();
        long tb = b.startMs();
        inA.pipeInput("room-1", a, Instant.ofEpochMilli(ta));
        inB.pipeInput("room-1", b, Instant.ofEpochMilli(tb));

        List<JsonNode> rows = new ArrayList<>();
        for (var kv : out.readKeyValuesToList()) {
            rows.add(MAPPER.readTree(kv.value));
        }

        assertEquals(1, rows.size(), () -> rows.toString());
        assertEquals("OVERLAPS", rows.get(0).path("relation").asText());
        assertEquals("room-1", rows.get(0).path("key").asText());
    }

    @Test
    void joinNonOverlappingButCloseStarts_stillClassifiesBefore() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        AllenInferenceTopology.build(builder);
        driver = new TopologyTestDriver(builder.build(), baseProps());

        IntervalRecord a = new IntervalRecord(0, 10);
        IntervalRecord b = new IntervalRecord(100, 200);

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
                        AllenInferenceTopology.TOPIC_OUT,
                        new StringDeserializer(),
                        new ByteArrayDeserializer());

        inA.pipeInput("k", a, Instant.ofEpochMilli(a.startMs()));
        inB.pipeInput("k", b, Instant.ofEpochMilli(b.startMs()));

        JsonNode one = MAPPER.readTree(out.readValue());
        assertEquals("BEFORE", one.path("relation").asText());
    }

    @Test
    void overlappingStartsFarApart_canBeMissedByJoinWindow() {
        StreamsBuilder builder = new StreamsBuilder();
        AllenInferenceTopology.build(builder);
        driver = new TopologyTestDriver(builder.build(), baseProps());

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
                        AllenInferenceTopology.TOPIC_OUT,
                        new StringDeserializer(),
                        new ByteArrayDeserializer());

        inA.pipeInput("k", a, Instant.ofEpochMilli(a.startMs()));
        inB.pipeInput("k", b, Instant.ofEpochMilli(b.startMs()));

        assertTrue(out.isEmpty(), "5m join window should not pair starts 999_000ms (~16.6 min) apart");
    }

    private static Properties baseProps() {
        Properties cfg = new Properties();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "allen-join-test-2");
        cfg.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return cfg;
    }
}
