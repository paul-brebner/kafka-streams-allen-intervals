package com.example.allenstreams;

import java.util.EnumSet;
import java.util.Objects;

/**
 * Composition of Allen's thirteen <b>basic</b> relations (Allen 1983).
 *
 * <p>If {@code A} stands in relation {@code ab} to {@code B}, and {@code B} stands in relation {@code bc}
 * to {@code C}, then the possible relations of {@code A} to {@code C} are exactly {@code compose(ab, bc)}.
 * Each cell is an {@link EnumSet} (possibly a singleton); some cells are the full set of thirteen relations.
 *
 * <p>For <b>general</b> relations (sets of basic relations), use {@link #composeSets(EnumSet, EnumSet)}
 * which distributes composition over unions in the standard way.
 * <p>Table source: standard interval algebra composition (e.g. Alspaugh, “Allen's Interval Algebra”,
 * Table 4a). Relation order for rows and columns matches {@link AllenRelation#ordinal()}: {@code
 * pmoFDseSdfOMP} ↔ {@code BEFORE, MEETS, OVERLAPS, FINISHED_BY, CONTAINS, STARTS, EQUALS, STARTED_BY,
 * DURING, FINISHES, OVERLAPPED_BY, MET_BY, AFTER}.
 */
public final class AllenComposition {

    /** Concurrent relations shorthand {@code (oFDseSdfO)} from the literature. */
    public static final EnumSet<AllenRelation> CONCUR =
            EnumSet.of(
                    AllenRelation.OVERLAPS,
                    AllenRelation.FINISHED_BY,
                    AllenRelation.CONTAINS,
                    AllenRelation.STARTS,
                    AllenRelation.EQUALS,
                    AllenRelation.STARTED_BY,
                    AllenRelation.DURING,
                    AllenRelation.FINISHES,
                    AllenRelation.OVERLAPPED_BY);

    private static final EnumSet<AllenRelation> ALL = EnumSet.allOf(AllenRelation.class);

    /** Row = first relation A–B, column = second relation B–C (Alspaugh notation in cells). */
    private static final String[][] CELLS = {
        {"p", "p", "p", "p", "p", "p", "p", "p", "pmosd", "pmosd", "pmosd", "pmosd", "full"},
        {"p", "p", "p", "p", "p", "m", "m", "m", "osd", "osd", "osd", "Fef", "DSOMP"},
        {"p", "p", "pmo", "pmo", "pmoFD", "o", "o", "oFD", "osd", "osd", "concur", "DSO", "DSOMP"},
        {"p", "m", "o", "F", "D", "o", "F", "D", "osd", "Fef", "DSO", "DSO", "DSOMP"},
        {"pmoFD", "oFD", "oFD", "D", "D", "oFD", "D", "D", "concur", "DSO", "DSO", "DSO", "DSOMP"},
        {"p", "p", "pmo", "pmo", "pmoFD", "s", "s", "seS", "d", "d", "dfO", "M", "P"},
        {"p", "m", "o", "F", "D", "s", "e", "S", "d", "f", "O", "M", "P"},
        {"pmoFD", "oFD", "oFD", "D", "D", "seS", "S", "S", "dfO", "O", "O", "M", "P"},
        {"p", "p", "pmosd", "pmosd", "full", "d", "d", "dfOMP", "d", "d", "dfOMP", "P", "P"},
        {"p", "m", "osd", "Fef", "DSOMP", "d", "f", "OMP", "d", "f", "OMP", "P", "P"},
        {"pmoFD", "oFD", "concur", "DSO", "DSOMP", "dfO", "O", "OMP", "dfO", "O", "OMP", "P", "P"},
        {"pmoFD", "seS", "dfO", "M", "P", "dfO", "M", "P", "dfO", "M", "P", "P", "P"},
        {"full", "dfOMP", "dfOMP", "P", "P", "dfOMP", "P", "P", "dfOMP", "P", "P", "P", "P"}
    };

    private static final EnumSet<AllenRelation>[][] TABLE = buildTable();

    @SuppressWarnings("unchecked")
    private static EnumSet<AllenRelation>[][] buildTable() {
        int n = AllenRelation.values().length;
        EnumSet<AllenRelation>[][] t = new EnumSet[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                t[i][j] = parseCell(CELLS[i][j]);
            }
        }
        return t;
    }

    /**
     * Possible relations of {@code A} to {@code C} given {@code A}–{@code B} is {@code ab} and {@code B}–{@code
     * C} is {@code bc}. Both arguments must be <b>basic</b> relations (the thirteen enum constants).
     */
    public static EnumSet<AllenRelation> compose(AllenRelation ab, AllenRelation bc) {
        Objects.requireNonNull(ab, "ab");
        Objects.requireNonNull(bc, "bc");
        return TABLE[ab.ordinal()][bc.ordinal()].clone();
    }

    static EnumSet<AllenRelation> parseCell(String spec) {
        String s = spec.trim();
        if ("full".equals(s)) {
            return ALL.clone();
        }
        if ("concur".equals(s)) {
            return CONCUR.clone();
        }
        EnumSet<AllenRelation> out = EnumSet.noneOf(AllenRelation.class);
        for (int i = 0; i < s.length(); i++) {
            out.add(charToRelation(s, i));
        }
        return out;
    }

    private static AllenRelation charToRelation(String token, int index) {
        char c = token.charAt(index);
        return switch (c) {
            case 'p' -> AllenRelation.BEFORE;
            case 'P' -> AllenRelation.AFTER;
            case 'm' -> AllenRelation.MEETS;
            case 'M' -> AllenRelation.MET_BY;
            case 'o' -> AllenRelation.OVERLAPS;
            case 'O' -> AllenRelation.OVERLAPPED_BY;
            case 'F' -> AllenRelation.FINISHED_BY;
            case 'f' -> AllenRelation.FINISHES;
            case 'D' -> AllenRelation.CONTAINS;
            case 'd' -> AllenRelation.DURING;
            case 's' -> AllenRelation.STARTS;
            case 'S' -> AllenRelation.STARTED_BY;
            case 'e' -> AllenRelation.EQUALS;
            default -> throw new IllegalArgumentException(
                    "Unknown relation char '" + c + "' in token \"" + token + "\" at index " + index);
        };
    }

    /**
     * Composition for <b>general</b> Allen relations expressed as sets of basics (union of primitives).
     * Defined as ⋃{@link #compose(AllenRelation, AllenRelation) compose}{@code (x, y) | x ∈ ab, y ∈ bc}}.
     */
    public static EnumSet<AllenRelation> composeSets(
            EnumSet<AllenRelation> ab, EnumSet<AllenRelation> bc) {
        Objects.requireNonNull(ab, "ab");
        Objects.requireNonNull(bc, "bc");
        if (ab.isEmpty() || bc.isEmpty()) {
            return EnumSet.noneOf(AllenRelation.class);
        }
        EnumSet<AllenRelation> acc = EnumSet.noneOf(AllenRelation.class);
        for (AllenRelation x : ab) {
            for (AllenRelation y : bc) {
                acc.addAll(compose(x, y));
            }
        }
        return acc;
    }

    private AllenComposition() {}
}
