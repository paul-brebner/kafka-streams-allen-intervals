package com.example.allenstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
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

/**
 * Demo / integration-style test: same inputs go through {@link AllenInferenceTopology} (join window) and
 * {@link AllenBufferedTopology} (per-key buffer). Run alone with:
 *
 * <pre>
 * mvn -q test -Dtest=AllenBufferedTopologyDemoTest
 * </pre>
 */
class AllenBufferedTopologyDemoTest {

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
    void demo_farStartOverlap_joinMisses_bufferEmitsOverlaps() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        AllenInferenceTopology.build(builder);
        AllenBufferedTopology.build(builder);

        Properties cfg = new Properties();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "allen-demo");
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

        TestOutputTopic<String, byte[]> joinOut =
                driver.createOutputTopic(
                        AllenInferenceTopology.TOPIC_OUT,
                        new StringDeserializer(),
                        new ByteArrayDeserializer());
        TestOutputTopic<String, byte[]> bufferOut =
                driver.createOutputTopic(
                        AllenBufferedTopology.TOPIC_OUT,
                        new StringDeserializer(),
                        new ByteArrayDeserializer());

        inA.pipeInput("room-1", a, Instant.ofEpochMilli(a.startMs()));
        inB.pipeInput("room-1", b, Instant.ofEpochMilli(b.startMs()));

        List<JsonNode> joinRows = decodeAll(joinOut);
        List<JsonNode> bufferRows = decodeAll(bufferOut);

        String banner =
                """
                === Allen intervals demo (TopologyTestDriver) ===
                Input A: [%d, %d]  Input B: [%d, %d]  (starts ~%d ms apart, intervals overlap)
                Join output (%s): %s
                Buffered output (%s): %s
                ===============================================
                """
                        .formatted(
                                a.startMs(),
                                a.endMs(),
                                b.startMs(),
                                b.endMs(),
                                b.startMs() - a.startMs(),
                                AllenInferenceTopology.TOPIC_OUT,
                                pretty(joinRows),
                                AllenBufferedTopology.TOPIC_OUT,
                                pretty(bufferRows));

        System.out.println(banner);

        assertTrue(joinRows.isEmpty(), "join window should not pair these starts");
        assertEquals(1, bufferRows.size(), () -> bufferRows.toString());
        assertEquals("OVERLAPS", bufferRows.get(0).path("relation").asText());
    }

    private static List<JsonNode> decodeAll(TestOutputTopic<String, byte[]> topic) throws Exception {
        List<JsonNode> rows = new ArrayList<>();
        for (var kv : topic.readKeyValuesToList()) {
            rows.add(MAPPER.readTree(new String(kv.value, StandardCharsets.UTF_8)));
        }
        return rows;
    }

    private static String pretty(List<JsonNode> rows) {
        if (rows.isEmpty()) {
            return "(no records)";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sb.append("; ");
            }
            sb.append(rows.get(i).toString());
        }
        return sb.toString();
    }
}
