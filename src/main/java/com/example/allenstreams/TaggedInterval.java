package com.example.allenstreams;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TaggedInterval(
        @JsonProperty("side") StreamSide side, @JsonProperty("interval") IntervalRecord interval) {}
