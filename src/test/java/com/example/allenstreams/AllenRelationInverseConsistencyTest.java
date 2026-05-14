package com.example.allenstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Ensures Allen <b>converses</b> line up: how {@code b} stands to {@code a} is the {@link AllenRelation#inverse()
 * inverse} of how {@code a} stands to {@code b}. The streaming and composition code rely on a fixed A–B / B–C
 * orientation; callers flip perspective by swapping arguments or calling {@code inverse()}.
 */
class AllenRelationInverseConsistencyTest {

    @Test
    void swappedIntervals_matchesEnumInverse() {
        List<IntervalRecord> universe = new ArrayList<>();
        int max = 13;
        for (int s = 0; s < max; s++) {
            for (int e = s + 1; e <= max; e++) {
                universe.add(new IntervalRecord(s, e));
            }
        }
        for (IntervalRecord a : universe) {
            for (IntervalRecord b : universe) {
                AllenRelation ab = AllenIntervalAlgebra.relation(a, b);
                AllenRelation ba = AllenIntervalAlgebra.relation(b, a);
                assertEquals(ab.inverse(), ba, () -> "a=" + a + " b=" + b);
                assertEquals(ba.inverse(), ab);
            }
        }
    }
}
