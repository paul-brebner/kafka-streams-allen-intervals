package com.example.allenstreams;

/**
 * The thirteen Allen relations of interval A to interval B (proper intervals: start before end).
 *
 * <p>Composition of relations along a shared middle interval is in {@link AllenComposition}.
 */
public enum AllenRelation {
    BEFORE,
    MEETS,
    OVERLAPS,
    FINISHED_BY,
    CONTAINS,
    STARTS,
    EQUALS,
    STARTED_BY,
    DURING,
    FINISHES,
    OVERLAPPED_BY,
    MET_BY,
    AFTER;

    public AllenRelation inverse() {
        return switch (this) {
            case BEFORE -> AFTER;
            case AFTER -> BEFORE;
            case MEETS -> MET_BY;
            case MET_BY -> MEETS;
            case OVERLAPS -> OVERLAPPED_BY;
            case OVERLAPPED_BY -> OVERLAPS;
            case DURING -> CONTAINS;
            case CONTAINS -> DURING;
            case STARTS -> STARTED_BY;
            case STARTED_BY -> STARTS;
            case FINISHES -> FINISHED_BY;
            case FINISHED_BY -> FINISHES;
            case EQUALS -> EQUALS;
        };
    }
}
