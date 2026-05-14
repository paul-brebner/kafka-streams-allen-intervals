package com.example.allenstreams;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;

/**
 * Joins two interval streams on the same key and emits the exact Allen relation for each candidate pair.
 *
 * <p>Record <b>event time</b> should be the interval <b>start</b> (see README): use {@code
 * ProducerRecord(..., startMs, ...)} or {@link org.apache.kafka.streams.TopologyTestDriver} timestamps
 * accordingly, so {@link JoinWindows} pruning approximates “temporally nearby” intervals.
 */
public final class AllenInferenceTopology {

    public static final String TOPIC_A = "intervals-a";
    public static final String TOPIC_B = "intervals-b";
    public static final String TOPIC_OUT = "allen-inferences";

    /** Half-width of the symmetric join window on the stream-time axis (see README). */
    public static final Duration JOIN_HALF_WIDTH = Duration.ofMinutes(5);

    public static void build(StreamsBuilder builder) {
        Serde<IntervalRecord> iv = IntervalSerde.serde();
        Consumed<String, IntervalRecord> consumed = Consumed.with(Serdes.String(), iv);
        KStream<String, IntervalRecord> left = builder.stream(TOPIC_A, consumed);
        KStream<String, IntervalRecord> right = builder.stream(TOPIC_B, consumed);

        JoinWindows windows = JoinWindows.ofTimeDifferenceWithNoGrace(JOIN_HALF_WIDTH);

        ValueJoinerWithKey<String, IntervalRecord, IntervalRecord, byte[]> joiner =
                (key, a, b) -> {
                    AllenRelation r = AllenIntervalAlgebra.relation(a, b);
                    try {
                        return IntervalSerde.inferenceToJsonBytes(key, r, a, b);
                    } catch (Exception e) {
                        throw new org.apache.kafka.common.errors.SerializationException(e);
                    }
                };

        left.join(right, joiner, windows, StreamJoined.with(Serdes.String(), iv, iv))
                .to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.ByteArray()));
    }

    private AllenInferenceTopology() {}
}
