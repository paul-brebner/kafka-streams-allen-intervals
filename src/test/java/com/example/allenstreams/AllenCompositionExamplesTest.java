package com.example.allenstreams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Explicit composition checks plus a printable mini-report.
 *
 * <pre>
 * mvn -q test -Dtest=AllenCompositionExamplesTest
 * </pre>
 */
class AllenCompositionExamplesTest {

    @Test
    void literature_meetsComposesMeets_toBefore() {
        assertEquals(EnumSet.of(AllenRelation.BEFORE), AllenComposition.compose(AllenRelation.MEETS, AllenRelation.MEETS));
    }

    @Test
    void literature_startsComposesOverlaps_toBeforeMeetsOverlaps() {
        assertEquals(
                EnumSet.of(AllenRelation.BEFORE, AllenRelation.MEETS, AllenRelation.OVERLAPS),
                AllenComposition.compose(AllenRelation.STARTS, AllenRelation.OVERLAPS));
    }

    @Test
    void beforeComposesBefore_stillBefore() {
        assertEquals(EnumSet.of(AllenRelation.BEFORE), AllenComposition.compose(AllenRelation.BEFORE, AllenRelation.BEFORE));
    }

    @Test
    void equalsComposesEquals_equalsOnly() {
        assertEquals(EnumSet.of(AllenRelation.EQUALS), AllenComposition.compose(AllenRelation.EQUALS, AllenRelation.EQUALS));
    }

    @Test
    void afterComposesBefore_isFullRelationSet() {
        assertEquals(EnumSet.allOf(AllenRelation.class), AllenComposition.compose(AllenRelation.AFTER, AllenRelation.BEFORE));
    }

    @Test
    void concur_hasNineBasics() {
        assertEquals(9, AllenComposition.CONCUR.size());
        assertTrue(AllenComposition.CONCUR.contains(AllenRelation.OVERLAPS));
        assertTrue(AllenComposition.CONCUR.contains(AllenRelation.EQUALS));
    }

    @Test
    void composeSets_unionsMembers() {
        EnumSet<AllenRelation> ab = EnumSet.of(AllenRelation.BEFORE, AllenRelation.MEETS);
        EnumSet<AllenRelation> bc = EnumSet.of(AllenRelation.MEETS);
        EnumSet<AllenRelation> got = AllenComposition.composeSets(ab, bc);
        EnumSet<AllenRelation> expected = EnumSet.noneOf(AllenRelation.class);
        expected.addAll(AllenComposition.compose(AllenRelation.BEFORE, AllenRelation.MEETS));
        expected.addAll(AllenComposition.compose(AllenRelation.MEETS, AllenRelation.MEETS));
        assertEquals(expected, got);
    }

    @Test
    void sampleOutput_printsCompositionSamples() {
        record Sample(AllenRelation ab, AllenRelation bc, String note) {}

        Sample[] rows = {
            new Sample(AllenRelation.MEETS, AllenRelation.MEETS, "touching chain tightens to precedes"),
            new Sample(AllenRelation.STARTS, AllenRelation.OVERLAPS, "Allen-style starts ∘ overlaps"),
            new Sample(AllenRelation.BEFORE, AllenRelation.OVERLAPS, "left interval still mostly left of unknown C"),
            new Sample(AllenRelation.DURING, AllenRelation.DURING, "nested middles"),
            new Sample(AllenRelation.AFTER, AllenRelation.BEFORE, "widest uncertainty (full set)"),
            new Sample(AllenRelation.EQUALS, AllenRelation.OVERLAPS, "A equals B that overlaps C"),
        };

        StringBuilder sb = new StringBuilder();
        sb.append("=== Allen composition samples (compose(A–B, B–C) → possible A–C) ===\n");
        for (Sample s : rows) {
            EnumSet<AllenRelation> out = AllenComposition.compose(s.ab(), s.bc());
            String rels =
                    out.stream().map(Enum::name).sorted().collect(Collectors.joining(", "));
            sb.append(String.format("%12s ∘ %-12s → {%s}%n", s.ab(), s.bc(), rels));
            sb.append("    (").append(s.note()).append(")\n");
        }
        sb.append("================================================================\n");
        System.out.println(sb);
    }
}
