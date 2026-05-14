package com.example.allenstreams;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable per-key buffer (serialized into {@link org.apache.kafka.streams.state.KeyValueStore}).
 * Jackson needs a default constructor and mutable lists for deserialization.
 */
public final class IntervalBuffers {

    @JsonProperty("sideA")
    private List<IntervalRecord> sideA = new ArrayList<>();

    @JsonProperty("sideB")
    private List<IntervalRecord> sideB = new ArrayList<>();

    public List<IntervalRecord> sideA() {
        return sideA;
    }

    public List<IntervalRecord> sideB() {
        return sideB;
    }

    public void setSideA(List<IntervalRecord> sideA) {
        this.sideA = sideA != null ? sideA : new ArrayList<>();
    }

    public void setSideB(List<IntervalRecord> sideB) {
        this.sideB = sideB != null ? sideB : new ArrayList<>();
    }
}
