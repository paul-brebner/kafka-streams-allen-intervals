# Allen composition table (complete 13×13)

This repo implements the **composition** operation for James Allen’s **thirteen basic** interval relations (1983). If **A** stands in relation **R₁** to **B**, and **B** stands in relation **R₂** to **C**, then the possible relations of **A** to **C** are exactly **R₁ ∘ R₂** (a set of basics, often a singleton).

- **Code:** `AllenComposition.compose(AllenRelation, AllenRelation)` returns `EnumSet<AllenRelation>`.
- **General relations** (unions of basics): `AllenComposition.composeSets(EnumSet, EnumSet)` distributes over members.
- **Correctness:** `AllenCompositionTest` exhaustively checks all triples of intervals on a small integer grid against `AllenIntervalAlgebra.relation`.

The Kafka Streams topologies in this project still only **emit pairwise** classifications; composition is available for **downstream reasoning** (constraint propagation, narrative chains, etc.).

## Tests

- **`AllenCompositionTest`** — brute force on all interval triples from a small integer grid; `composeSets` vs `compose` on singletons.
- **`AllenCompositionExamplesTest`** — literature-style spot checks + printable samples. Run alone:

```bash
mvn -q test -Dtest=AllenCompositionExamplesTest
```

### Sample stdout (`sampleOutput_printsCompositionSamples`)

```
=== Allen composition samples (compose(A–B, B–C) → possible A–C) ===
       MEETS ∘ MEETS        → {BEFORE}
    (touching chain tightens to precedes)
      STARTS ∘ OVERLAPS     → {BEFORE, MEETS, OVERLAPS}
    (Allen-style starts ∘ overlaps)
      BEFORE ∘ OVERLAPS     → {BEFORE}
    (left interval still mostly left of unknown C)
      DURING ∘ DURING       → {DURING}
    (nested middles)
       AFTER ∘ BEFORE       → {AFTER, BEFORE, CONTAINS, DURING, EQUALS, FINISHED_BY, FINISHES, MEETS, MET_BY, OVERLAPPED_BY, OVERLAPS, STARTED_BY, STARTS}
    (widest uncertainty (full set))
      EQUALS ∘ OVERLAPS     → {OVERLAPS}
    (A equals B that overlaps C)
================================================================
```

(Relation names in the `AFTER ∘ BEFORE` line are sorted alphabetically in the test; the set is all thirteen relations.)

## Converses (“inverses”)

- **Pairwise classification:** `AllenIntervalAlgebra.relation(a, b)` is always **“how *a* relates to *b*”**. The converse view **“how *b* relates to *a*”** is `relation(b, a)`, which equals `relation(a, b).inverse()` — see `AllenRelation#inverse` and **`AllenRelationInverseConsistencyTest`**.
- **Kafka Streams:** payloads label stream **A** vs **B** explicitly; we do not auto-flip to the other endpoint’s perspective.
- **Composition:** `AllenComposition.compose` is defined for chains **A–B–C** in that order. Re-orienting a chain (e.g. reasoning from **C** backward) means applying **`inverse()`** to stored relations and/or using the correct transpose of the composition table (not wrapped as a single helper here).

## Notation (Alspaugh / Allen)

Row and column order is **`pmoFDseSdfOMP`** (same as `AllenRelation` enum declaration order):

| Symbol | `AllenRelation` |
|--------|-----------------|
| `p` | `BEFORE` |
| `m` | `MEETS` |
| `o` | `OVERLAPS` |
| `F` | `FINISHED_BY` |
| `D` | `CONTAINS` |
| `s` | `STARTS` |
| `e` | `EQUALS` |
| `S` | `STARTED_BY` |
| `d` | `DURING` |
| `f` | `FINISHES` |
| `O` | `OVERLAPPED_BY` |
| `M` | `MET_BY` |
| `P` | `AFTER` |

**Shorthand in cells**

- A **string of symbols** is the **set** of those relations, e.g. `pmo` = {before, meets, overlaps}.
- **`full`** = all thirteen relations (`EnumSet.allOf(AllenRelation.class)`).
- **`concur`** = the nine-relation “concurrent” set `(oFDseSdfO)` — see `AllenComposition.CONCUR` in code.

Primary reference: [Thomas A. Alspaugh, *Allen's Interval Algebra*](https://thomasalspaugh.org/pub/fnd/allen.html) (Table 4a). The embedded `CELLS` matrix in `AllenComposition.java` matches that table.

## Composition table (row ∘ column)

**Row** = relation **A–B** (first argument to `compose`). **Column** = relation **B–C** (second argument). Cell = possible relations **A–C** (Alspaugh shorthand / `full` / `concur`).

| ∘ | p | m | o | F | D | s | e | S | d | f | O | M | P |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **p** | p | p | p | p | p | p | p | p | pmosd | pmosd | pmosd | pmosd | full |
| **m** | p | p | p | p | p | m | m | m | osd | osd | osd | Fef | DSOMP |
| **o** | p | p | pmo | pmo | pmoFD | o | o | oFD | osd | osd | concur | DSO | DSOMP |
| **F** | p | m | o | F | D | o | F | D | osd | Fef | DSO | DSO | DSOMP |
| **D** | pmoFD | oFD | oFD | D | D | oFD | D | D | concur | DSO | DSO | DSO | DSOMP |
| **s** | p | p | pmo | pmo | pmoFD | s | s | seS | d | d | dfO | M | P |
| **e** | p | m | o | F | D | s | e | S | d | f | O | M | P |
| **S** | pmoFD | oFD | oFD | D | D | seS | S | S | dfO | O | O | M | P |
| **d** | p | p | pmosd | pmosd | full | d | d | dfOMP | d | d | dfOMP | P | P |
| **f** | p | m | osd | Fef | DSOMP | d | f | OMP | d | f | OMP | P | P |
| **O** | pmoFD | oFD | concur | DSO | DSOMP | dfO | O | OMP | dfO | O | OMP | P | P |
| **M** | pmoFD | seS | dfO | M | P | dfO | M | P | dfO | M | P | P | P |
| **P** | full | dfOMP | dfOMP | P | P | dfOMP | P | P | dfOMP | P | P | P | P |

### Reading one cell (example)

Allen (via TIME 2017 paper) notes: if **starts(A,B)** and **overlaps(B,C)**, then **A** vs **C** may be **before**, **overlaps**, or **meets** — i.e. `compose(STARTS, OVERLAPS)` should include those three. In the table, row **`s`**, column **`o`**, the cell is **`pmo`**, which matches.

## API summary

| Method | Purpose |
|--------|---------|
| `AllenComposition.compose(R1, R2)` | Basic ∘ basic → `EnumSet` of possible **A–C** relations. |
| `AllenComposition.composeSets(S1, S2)` | For general relations **S1**, **S2** (sets of basics), ⋃ compose(x,y). |
| `AllenComposition.CONCUR` | The fixed nine-relation concurrent set. |

## Relation to streaming

- **Streams path:** each message is still one **pairwise** `relation(intervalA, intervalB)`.
- **Composition:** use when you have **chains** or **partial knowledge** (general relations) and need implied constraints between **non-adjacent** intervals sharing a middle.

Further reading: James F. Allen, *Maintaining Knowledge about Temporal Intervals*, **Commun. ACM** 26(11), 1983.
