package com.example.allenstreams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Buffers recent intervals per key on each side, compares each arrival against the opposite buffer, and
 * emits one output record per pair (Allen relation in JSON). Bounded by {@link #maxPerSide} evictions
 * (oldest dropped first).
 */
public final class AllenBufferTransformer
        implements Transformer<String, TaggedInterval, Iterable<KeyValue<String, byte[]>>> {

    private final String storeName;
    private final int maxPerSide;

    private KeyValueStore<String, IntervalBuffers> store;

    public AllenBufferTransformer(String storeName, int maxPerSide) {
        this.storeName = storeName;
        this.maxPerSide = maxPerSide;
    }

    @Override
    public void init(ProcessorContext context) {
        this.store = context.getStateStore(storeName);
    }

    @Override
    public Iterable<KeyValue<String, byte[]>> transform(String key, TaggedInterval tagged) {
        if (key == null || tagged == null) {
            return Collections.emptyList();
        }
        IntervalBuffers buf = store.get(key);
        if (buf == null) {
            buf = new IntervalBuffers();
        }
        List<KeyValue<String, byte[]>> out = new ArrayList<>();
        IntervalRecord incoming = tagged.interval();
        try {
            if (tagged.side() == StreamSide.A) {
                for (IntervalRecord other : buf.sideB()) {
                    AllenRelation r = AllenIntervalAlgebra.relation(incoming, other);
                    out.add(KeyValue.pair(key, IntervalSerde.inferenceToJsonBytes(key, r, incoming, other)));
                }
                buf.sideA().add(incoming);
                trim(buf.sideA());
            } else {
                for (IntervalRecord other : buf.sideA()) {
                    AllenRelation r = AllenIntervalAlgebra.relation(other, incoming);
                    out.add(KeyValue.pair(key, IntervalSerde.inferenceToJsonBytes(key, r, other, incoming)));
                }
                buf.sideB().add(incoming);
                trim(buf.sideB());
            }
        } catch (Exception e) {
            throw new SerializationException(e);
        }
        store.put(key, buf);
        return out;
    }

    private void trim(List<IntervalRecord> list) {
        while (list.size() > maxPerSide) {
            list.remove(0);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
