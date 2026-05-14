package com.example.allenstreams;

import com.fasterxml.jackson.annotation.JsonProperty;

/** A closed interval on the millisecond line; requires {@code startMs < endMs}. */
public record IntervalRecord(
        @JsonProperty("startMs") long startMs, @JsonProperty("endMs") long endMs) {

    public IntervalRecord {
        if (!(startMs < endMs)) {
            throw new IllegalArgumentException("require startMs < endMs");
        }
    }
}
