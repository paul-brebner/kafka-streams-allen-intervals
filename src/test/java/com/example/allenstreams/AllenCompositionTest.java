package com.example.allenstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Validates {@link AllenComposition} against exhaustive triples of intervals on a small integer grid. */
class AllenCompositionTest {

    @Test
    void everyWitnessedTriple_respectsCompositionTable() {
        List<IntervalRecord> universe = new ArrayList<>();
        int max = 13;
        for (int s = 0; s < max; s++) {
            for (int e = s + 1; e <= max; e++) {
                universe.add(new IntervalRecord(s, e));
            }
        }
        for (IntervalRecord a : universe) {
            for (IntervalRecord b : universe) {
                for (IntervalRecord c : universe) {
                    AllenRelation rab = AllenIntervalAlgebra.relation(a, b);
                    AllenRelation rbc = AllenIntervalAlgebra.relation(b, c);
                    AllenRelation rac = AllenIntervalAlgebra.relation(a, c);
                    assertTrue(
                            AllenComposition.compose(rab, rbc).contains(rac),
                            () -> "a=" + a + " b=" + b + " c=" + c + " rab=" + rab + " rbc=" + rbc + " rac=" + rac);
                }
            }
        }
    }

    @Test
    void composeSets_onSingletons_matchesCompose() {
        for (AllenRelation x : AllenRelation.values()) {
            for (AllenRelation y : AllenRelation.values()) {
                assertEquals(
                        AllenComposition.compose(x, y),
                        AllenComposition.composeSets(EnumSet.of(x), EnumSet.of(y)));
            }
        }
    }
}
