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

## How to infer Allen relations over streams (pattern in this repo)

1. **Model** each event’s value as a proper interval `startMs < endMs`.
2. Set **record timestamp** to **`startMs`** via a **`TimestampExtractor`** (see `IntervalStartTimestampExtractor`). That makes the join’s time predicate align with “things that *start* near each other,” which is a **heuristic**, not Allen-complete.
3. Use a **KStream–KStream inner join** with a **symmetric `JoinWindows`** (here **five minutes** each side of the left record’s stream time via `ofTimeDifferenceWithNoGrace`) so only **start times within that horizon** become join candidates.
4. In a **`ValueJoiner`**, read **both values’** `[start,end]` and compute the **exact** Allen relation with `AllenIntervalAlgebra.relation(...)`.
5. Emit the relation (and optional metadata) to an output topic for downstream rules, metrics, or alerts.

**False negatives:** if two intervals **overlap** but their **starts** differ by more than the join window, Streams will **never** pair them—you need a larger window, a different timestamp strategy, or a **stateful sweep** / secondary index (not shown here).

**False positives (candidates only):** the join may emit pairs that are **`before`** or **`after`** in Allen terms if their starts are close but intervals do not overlap—the joiner can **drop** or **label** them (this example emits all exact relations, including `before` / `after`).

## Buffered transformer path (no wide join window)

`AllenInferenceTopology` keeps the **join-window** story for comparison. **`AllenBufferedTopology`** is the next step: it **merges** the same two input topics after tagging each record as side **A** or **B**, uses a **`KeyValueStore`** per key to hold **bounded lists** of recent intervals on each side, and on each arrival runs **`AllenIntervalAlgebra.relation(...)`** against **every** interval on the opposite side. Emission uses **`KStream#flatTransform`** so one input can produce **zero or many** JSON outputs.

That fixes the classic join false negative where **overlapping** intervals have **starts** farther apart than `JoinWindows` allows: `AllenBufferedTopologyTest` drives the same “999s apart starts, still overlapping” scenario and expects **`OVERLAPS`** on topic **`allen-inferences-buffered`**, while `AllenInferenceTopologyTest` still documents that the **join** path stays empty.

**Trade-off:** buffering is **memory-bounded** (`MAX_PER_SIDE` per key per side, oldest evicted first). Pairs involving an interval that was already evicted are **not** rediscovered when a late counterpart arrives—you would need a different retention policy, spill-to-disk, or interval-index structure for “complete” history.

## Build & test

```bash
cd /Users/pbrebner/Applications/Experiments/kafka-streams-allen-intervals
mvn -q test
```

The test uses **`TopologyTestDriver`** (no broker).

The **`llm-consumer/`** module is built separately (`cd llm-consumer && mvn -q package`) and needs a real broker + Ollama; see [`docs/llm-consumer-smoke-run.md`](docs/llm-consumer-smoke-run.md).

## Documentation in this repo

- [`docs/use-cases-and-value.md`](docs/use-cases-and-value.md) — whether this example is “useful,” and realistic use cases.
- [`docs/test-console-output.md`](docs/test-console-output.md) — captured stdout from selected demo tests.
- [`docs/linkedin-intro-allen-kafka-streams.md`](docs/linkedin-intro-allen-kafka-streams.md) — short LinkedIn-style intro draft.
- [`docs/local-llm-ollama.md`](docs/local-llm-ollama.md) — optional **local** LLM narration via Ollama + `scripts/explain_inference_ollama.py`.
- [`docs/llm-java-consumer.md`](docs/llm-java-consumer.md) — **separate Java Kafka consumer** (`llm-consumer/`) that reads inference topics and calls Ollama.
- [`docs/llm-consumer-smoke-run.md`](docs/llm-consumer-smoke-run.md) — **end-to-end smoke run**: sample Kafka payloads, `java -jar` command, **captured LLM log output**.

## Related

- Sibling **Python** reference for the thirteen relations only: `../allen-interval-algebra/`.
- James F. Allen, *Maintaining Knowledge about Temporal Intervals*, **Commun. ACM** (classic reference for the thirteen relations and composition tables).
