package com.example.allenstreams;

/** Closed-interval Allen classification (same rules as the Python reference repo). */
public final class AllenIntervalAlgebra {

    private AllenIntervalAlgebra() {}

    /** Allen relation of {@code a} to {@code b} (how {@code a} stands with respect to {@code b}). */
    public static AllenRelation relation(IntervalRecord a, IntervalRecord b) {
        double a0 = a.startMs();
        double a1 = a.endMs();
        double b0 = b.startMs();
        double b1 = b.endMs();

        if (a1 < b0) {
            return AllenRelation.BEFORE;
        }
        if (b1 < a0) {
            return AllenRelation.AFTER;
        }
        if (a0 == b0 && a1 == b1) {
            return AllenRelation.EQUALS;
        }
        if (a1 == b0) {
            return AllenRelation.MEETS;
        }
        if (a0 == b1) {
            return AllenRelation.MET_BY;
        }
        if (a0 < b0 && a1 < b1 && b0 < a1) {
            return AllenRelation.OVERLAPS;
        }
        if (b0 < a0 && b1 < a1 && a0 < b1) {
            return AllenRelation.OVERLAPPED_BY;
        }
        if (a0 == b0 && a1 < b1) {
            return AllenRelation.STARTS;
        }
        if (a0 == b0 && a1 > b1) {
            return AllenRelation.STARTED_BY;
        }
        if (a1 == b1 && a0 > b0) {
            return AllenRelation.FINISHES;
        }
        if (a1 == b1 && a0 < b0) {
            return AllenRelation.FINISHED_BY;
        }
        if (b0 < a0 && a1 < b1) {
            return AllenRelation.DURING;
        }
        if (a0 < b0 && b1 < a1) {
            return AllenRelation.CONTAINS;
        }
        throw new IllegalStateException("unclassified intervals a=" + a + " b=" + b);
    }
}
