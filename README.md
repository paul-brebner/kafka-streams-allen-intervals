# Kafka Streams and Allen’s interval relations

This repo is a **small Java example** of how you can **infer Allen’s thirteen interval-to-interval relations** over **two KStreams** of intervals, while being honest about **what Kafka Streams already gives you** versus what **you** must add.

## What Kafka Streams already “supports” (indirectly)

Kafka Streams does **not** implement Allen’s algebra. It **does** give you primitives that often sit underneath temporal reasoning:

| Streams primitive | Role w.r.t. Allen-style thinking |
|-------------------|----------------------------------|
| **Event-time** per record | You choose what the record timestamp means (often **interval start** in this example). |
| **Windows** (tumbling, hopping, sliding, **session**) | Each window is an **interval on the timeline**; window overlap / adjacency is **Allen-like** *for those synthetic intervals*, not for arbitrary domain intervals unless you map them. |
| **Grace / suppression** | Control how late events interact with **window boundaries**—temporal policy, not Allen composition. |
| **KStream–KStream `join(..., JoinWindows, ...)`** | Joins **same-key** records whose **stream timestamps** fall in a **relative time window** (scalar comparison), *not* full closed-interval overlap in general. |

So: Streams is strong at **ordered time**, **partition-local state**, and **time-bounded joins**; Allen is about **comparing two value-bounded intervals** `[start, end]`. You bridge the gap in application code.

## Problem and solution (limited join window)

**Problem:** `JoinWindows` only pairs records when **stream timestamps** are within the window. Two intervals can **overlap in value** (`[startMs, endMs]`) but have **starts too far apart**, so the **join never fires**—a **false negative** for Allen-style overlap.

**Solution:** **`AllenBufferedTopology`**—tag A/B, **merge**, **`KeyValueStore`** buffers per key, **`flatTransform`** compares each new interval to the **opposite** buffer and emits inferences. Pairing follows **retained interval geometry** (with a **bounded** buffer), not a **wider** join window.

## How to infer Allen relations over streams (pattern in this repo)

1. **Model** each event’s value as a proper interval `startMs < endMs`.
2. Set **record timestamp** to **`startMs`** via a **`TimestampExtractor`** (see `IntervalStartTimestampExtractor`). That makes the join’s time predicate align with “things that *start* near each other,” which is a **heuristic**, not Allen-complete.
3. Use a **KStream–KStream inner join** with a **symmetric `JoinWindows`** (here **five minutes** each side of the left record’s stream time via `ofTimeDifferenceWithNoGrace`) so only **start times within that horizon** become join candidates.
4. In a **`ValueJoiner`**, read **both values’** `[start,end]` and compute the **exact** Allen relation with `AllenIntervalAlgebra.relation(...)`.
5. Emit the relation (and optional metadata) to an output topic for downstream rules, metrics, or alerts.

**Composition (chains A–B–C):** the full **13×13 basic-relation composition table** and API live in [`AllenComposition`](src/main/java/com/example/allenstreams/AllenComposition.java); see [`docs/allen-composition.md`](docs/allen-composition.md). Kafka Streams in this repo still emits **pairwise** inferences only; composition is for downstream constraint reasoning.

**How composition is implemented (brief):** we **do not** re-prove compositions inside the JVM. The standard matrix (e.g. Alspaugh Table 4a) is **embedded** as string cells—each expands to an **`EnumSet<AllenRelation>`** (`full` = all thirteen, **`concur`** = the nine-relation shorthand, otherwise **letter codes** **`pmoFDseSdfOMP`** matching enum order). **`compose(R₁, R₂)`** is a **precomputed lookup**; **`composeSets`** unions **`compose`** over pairs when **R₁** / **R₂** are sets of basics. Correctness is enforced by **`AllenCompositionTest`**, which exhaustively checks integer-interval triples so **`relation(A,C)`** always lies in **`compose(relation(A,B), relation(B,C))`**—the same **`AllenIntervalAlgebra`** used for streaming pairs.

**False negatives:** if two intervals **overlap** but their **starts** differ by more than the join window, Streams will **never** pair them—you need a larger window, a different timestamp strategy, or a **stateful sweep** / secondary index (not shown here).

**False positives (candidates only):** the join may emit pairs that are **`before`** or **`after`** in Allen terms if their starts are close but intervals do not overlap—the joiner can **drop** or **label** them (this example emits all exact relations, including `before` / `after`).

## Buffered transformer path (no wide join window)

`AllenInferenceTopology` keeps the **join-window** story for comparison. **`AllenBufferedTopology`** is the next step: it **merges** the same two input topics after tagging each record as side **A** or **B**, uses a **`KeyValueStore`** per key to hold **bounded lists** of recent intervals on each side, and on each arrival runs **`AllenIntervalAlgebra.relation(...)`** against **every** interval on the opposite side. Emission uses **`KStream#flatTransform`** so one input can produce **zero or many** JSON outputs.

That fixes the classic join false negative where **overlapping** intervals have **starts** farther apart than `JoinWindows` allows: `AllenBufferedTopologyTest` drives the same “999s apart starts, still overlapping” scenario and expects **`OVERLAPS`** on topic **`allen-inferences-buffered`**, while `AllenInferenceTopologyTest` still documents that the **join** path stays empty.

**Trade-off:** buffering is **memory-bounded** (`MAX_PER_SIDE` per key per side, oldest evicted first). Pairs involving an interval that was already evicted are **not** rediscovered when a late counterpart arrives—you would need a different retention policy, spill-to-disk, or interval-index structure for “complete” history.

## Summary: how it works and what we fixed

**Behaviour.** Two input topics—**`intervals-a`** and **`intervals-b`**—carry **`IntervalRecord`** JSON (`startMs`, `endMs`). The repo implements **two** Kafka Streams paths on the same keys: (1) **`AllenInferenceTopology`** pairs records with **`JoinWindows`** (symmetric five minutes, `ofTimeDifferenceWithNoGrace`) and a **`ValueJoiner`** that runs **`AllenIntervalAlgebra.relation(a, b)`**, writing JSON to **`allen-inferences`**; (2) **`AllenBufferedTopology`** tags A/B, **merges**, uses a **`KeyValueStore`** (`allen-interval-buffers`) and **`flatTransform`** / **`AllenBufferTransformer`** to compare each arrival to the **opposite** buffer and emit **zero or many** JSON rows to **`allen-inferences-buffered`**. **`AllenComposition`** adds the full **13×13 basic-relation composition** table for **A–B–C** style reasoning (not wired into the topology by default). A separate **`llm-consumer`** JAR (plus optional **`scripts/explain_inference_ollama.py`**) can call **Ollama** on inference JSON.

**Issues resolved along the way.** **`JoinWindows`** arity for Kafka **3.9** (`ofTimeDifferenceWithNoGrace` vs a non-existent three-argument form). Clarifying the **join vs Allen geometry** gap: **overlapping** intervals with **starts** outside the window produce a **join false negative**, motivating the **buffer** path (covered in tests and demos). **DSL / lambda naming** to avoid shadowing **`StreamsBuilder`** and interval parameters. **Test robustness** (JSON parsing without awkward checked exceptions in lambdas). Using **`flatTransform`** for **multiple outputs per input**. **`llm-consumer`**: **effectively final** producer for shutdown hooks, **`wakeup`**, **`flush`** before close, and a default **Ollama model** that matches typical installs (**`llama3:latest`**). **Composition** table validated with **exhaustive interval-triple** checks against **`AllenIntervalAlgebra`**.

## Build & test

```bash
cd /Users/pbrebner/Applications/Experiments/kafka-streams-allen-intervals
mvn -q test
```

The test uses **`TopologyTestDriver`** (no broker).

The **`llm-consumer/`** module is built separately (`cd llm-consumer && mvn -q package`) and needs a real broker + Ollama; see [`docs/llm-consumer-smoke-run.md`](docs/llm-consumer-smoke-run.md).

## Documentation in this repo

- [`docs/application-topology.md`](docs/application-topology.md) — **Mermaid topology**: inputs, both Kafka Streams paths, outputs, `llm-consumer`, Ollama, optional Python script.
- [`docs/allen-composition.md`](docs/allen-composition.md) — **complete 13×13 composition table** (Alspaugh notation) + `AllenComposition` API.
- [`docs/test-console-output.md`](docs/test-console-output.md) — captured stdout from selected demo tests.
- [`docs/linkedin-intro-allen-kafka-streams.md`](docs/linkedin-intro-allen-kafka-streams.md) — short LinkedIn-style intro draft.
- [`docs/local-llm-ollama.md`](docs/local-llm-ollama.md) — optional **local** LLM narration via Ollama + `scripts/explain_inference_ollama.py`.
- [`docs/llm-java-consumer.md`](docs/llm-java-consumer.md) — **separate Java Kafka consumer** (`llm-consumer/`) that reads inference topics and calls Ollama.
- [`docs/llm-consumer-smoke-run.md`](docs/llm-consumer-smoke-run.md) — **end-to-end smoke run**: sample Kafka payloads, `java -jar` command, **captured LLM log output**.

## Related

- Sibling **Python** reference for the thirteen relations only: `../allen-interval-algebra/`.
- James F. Allen, *Maintaining Knowledge about Temporal Intervals*, **Commun. ACM** (thirteen relations and **composition**; see [`docs/allen-composition.md`](docs/allen-composition.md) for the table used here).
- Alspaugh reference for the published composition grid: [Allen's Interval Algebra](https://thomasalspaugh.org/pub/fnd/allen.html) (Table 4a).
