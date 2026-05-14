package com.example.allenstreams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Stateful path: merge tagged A/B intervals, buffer per key, and emit Allen relations for each new
 * interval against the opposite buffer. Does <b>not</b> use {@link org.apache.kafka.streams.kstream.JoinWindows}.
 *
 * <p>Trade-off: memory-bounded history per key per side ({@link #MAX_PER_SIDE}); evicted intervals are
 * no longer compared to future arrivals.
 */
public final class AllenBufferedTopology {

    /** Same input topics as {@link AllenInferenceTopology}. */
    public static final String TOPIC_OUT = "allen-inferences-buffered";

    public static final String BUFFER_STORE = "allen-interval-buffers";

    /** Oldest intervals dropped per side when the buffer exceeds this size. */
    public static final int MAX_PER_SIDE = 256;

    public static void build(StreamsBuilder builder) {
        Serde<IntervalRecord> iv = IntervalSerde.serde();
        Consumed<String, IntervalRecord> consumed = Consumed.with(Serdes.String(), iv);

        KStream<String, IntervalRecord> streamA = builder.stream(AllenInferenceTopology.TOPIC_A, consumed);
        KStream<String, IntervalRecord> streamB = builder.stream(AllenInferenceTopology.TOPIC_B, consumed);

        KStream<String, TaggedInterval> taggedA =
                streamA.mapValues(v -> new TaggedInterval(StreamSide.A, v), Named.as("tag-intervals-a"));
        KStream<String, TaggedInterval> taggedB =
                streamB.mapValues(v -> new TaggedInterval(StreamSide.B, v), Named.as("tag-intervals-b"));

        KStream<String, TaggedInterval> merged =
                taggedA.merge(taggedB, Named.as("merge-tagged-intervals"));

        StoreBuilder<KeyValueStore<String, IntervalBuffers>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(BUFFER_STORE),
                        Serdes.String(),
                        IntervalBuffersSerde.serde());

        builder.addStateStore(storeBuilder);

        merged.flatTransform(
                        () -> new AllenBufferTransformer(BUFFER_STORE, MAX_PER_SIDE),
                        Named.as("allen-buffer-flat-transform"),
                        BUFFER_STORE)
                .to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.ByteArray()));
    }

    private AllenBufferedTopology() {}
}
