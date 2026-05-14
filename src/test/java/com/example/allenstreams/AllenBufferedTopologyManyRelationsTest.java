package com.example.allenstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
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
 * Buffered topology drives Allen classification from <b>value intervals</b> alone (no join window).
 * This test feeds one (A, B) pair per key for each of the thirteen relations, then one richer case
 * where a single B is compared against two buffered As.
 *
 * <pre>
 * mvn -q test -Dtest=AllenBufferedTopologyManyRelationsTest
 * </pre>
 */
class AllenBufferedTopologyManyRelationsTest {

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
    void bufferedTopology_coversAllThirteenRelations_plusMultiBuffer() throws Exception {
        List<Case> cases =
                List.of(
                        new Case("r-BEFORE", new IntervalRecord(0, 10), new IntervalRecord(20, 30), AllenRelation.BEFORE),
                        new Case("r-MEETS", new IntervalRecord(0, 10), new IntervalRecord(10, 20), AllenRelation.MEETS),
                        new Case(
                                "r-OVERLAPS",
                                new IntervalRecord(0, 100),
                                new IntervalRecord(50, 150),
                                AllenRelation.OVERLAPS),
                        new Case(
                                "r-FINISHED_BY",
                                new IntervalRecord(10, 20),
                                new IntervalRecord(15, 20),
                                AllenRelation.FINISHED_BY),
                        new Case(
                                "r-CONTAINS",
                                new IntervalRecord(0, 30),
                                new IntervalRecord(10, 20),
                                AllenRelation.CONTAINS),
                        new Case("r-STARTS", new IntervalRecord(0, 10), new IntervalRecord(0, 20), AllenRelation.STARTS),
                        new Case(
                                "r-EQUALS",
                                new IntervalRecord(5, 15),
                                new IntervalRecord(5, 15),
                                AllenRelation.EQUALS),
                        new Case(
                                "r-STARTED_BY",
                                new IntervalRecord(0, 20),
                                new IntervalRecord(0, 10),
                                AllenRelation.STARTED_BY),
                        new Case(
                                "r-DURING",
                                new IntervalRecord(12, 18),
                                new IntervalRecord(10, 25),
                                AllenRelation.DURING),
                        new Case(
                                "r-FINISHES",
                                new IntervalRecord(15, 20),
                                new IntervalRecord(10, 20),
                                AllenRelation.FINISHES),
                        new Case(
                                "r-OVERLAPPED_BY",
                                new IntervalRecord(50, 200),
                                new IntervalRecord(0, 100),
                                AllenRelation.OVERLAPPED_BY),
                        new Case(
                                "r-MET_BY",
                                new IntervalRecord(10, 20),
                                new IntervalRecord(0, 10),
                                AllenRelation.MET_BY),
                        new Case(
                                "r-AFTER",
                                new IntervalRecord(100, 110),
                                new IntervalRecord(0, 50),
                                AllenRelation.AFTER));

        for (Case c : cases) {
            assertEquals(
                    c.expected(),
                    AllenIntervalAlgebra.relation(c.a(), c.b()),
                    "algebra sanity: " + c.key());
        }

        StreamsBuilder builder = new StreamsBuilder();
        AllenBufferedTopology.build(builder);

        Properties cfg = new Properties();
        cfg.put(StreamsConfig.APPLICATION_ID_CONFIG, "allen-many-relations");
        cfg.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        driver = new TopologyTestDriver(builder.build(), cfg);

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

        for (Case c : cases) {
            inA.pipeInput(c.key(), c.a(), Instant.ofEpochMilli(c.a().startMs()));
            inB.pipeInput(c.key(), c.b(), Instant.ofEpochMilli(c.b().startMs()));
        }

        // One B compared against two buffered As on the same key → two JSON rows.
        String multiKey = "r-MULTI-BUFFER";
        inA.pipeInput(multiKey, new IntervalRecord(0, 100), Instant.ofEpochMilli(0));
        inA.pipeInput(multiKey, new IntervalRecord(200, 300), Instant.ofEpochMilli(200));
        inB.pipeInput(multiKey, new IntervalRecord(250, 280), Instant.ofEpochMilli(250));

        List<JsonNode> rows = new ArrayList<>();
        for (var kv : out.readKeyValuesToList()) {
            rows.add(MAPPER.readTree(new String(kv.value, StandardCharsets.UTF_8)));
        }

        Map<String, List<JsonNode>> byKey = new HashMap<>();
        for (JsonNode row : rows) {
            String key = row.path("key").asText();
            byKey.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        EnumMap<AllenRelation, String> seen = new EnumMap<>(AllenRelation.class);
        for (Case c : cases) {
            List<JsonNode> forKey = byKey.get(c.key());
            assertEquals(1, forKey.size(), () -> "key=" + c.key() + " rows=" + forKey);
            String rel = forKey.get(0).path("relation").asText();
            assertEquals(c.expected().name(), rel, () -> "key=" + c.key());
            seen.put(c.expected(), c.key());
        }

        assertEquals(EnumSet.allOf(AllenRelation.class), seen.keySet());

        List<JsonNode> multi = byKey.get(multiKey);
        assertEquals(2, multi.size(), () -> multi.toString());
        Set<String> multiRels =
                multi.stream().map(r -> r.path("relation").asText()).collect(Collectors.toSet());
        assertEquals(Set.of("BEFORE", "CONTAINS"), multiRels);

        String summary =
                cases.stream()
                        .map(c -> c.key() + " → " + c.expected().name())
                        .collect(Collectors.joining("\n"));
        String multiLine =
                multi.stream()
                        .map(r -> "  " + r.path("relation").asText() + "  a=" + r.path("a") + "  b=" + r.path("b"))
                        .collect(Collectors.joining("\n"));

        System.out.println(
                """
                === Buffered Allen relations (13 + multi-buffer) ===
                """
                        + summary
                        + "\n"
                        + multiKey
                        + ":\n"
                        + multiLine
                        + "\n========================================================");

        assertTrue(rows.size() >= 15);
    }

    private record Case(String key, IntervalRecord a, IntervalRecord b, AllenRelation expected) {}
}
