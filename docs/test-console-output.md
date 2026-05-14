# Captured test console output

Stdout from selected `TopologyTestDriver` tests (SLF4J / RocksDB warnings from Maven are omitted).

**Java LLM consumer + Kafka + Ollama:** see [`docs/llm-consumer-smoke-run.md`](llm-consumer-smoke-run.md) (includes sample payloads under `scripts/sample-inferences-ndjson.txt`).

Regenerate:

```bash
cd /Users/pbrebner/Applications/Experiments/kafka-streams-allen-intervals
mvn -q test -Dtest=AllenBufferedTopologyManyRelationsTest
mvn -q test -Dtest=AllenBufferedTopologyDemoTest
```

LLM consumer smoke (Kafka + Ollama): follow [`docs/llm-consumer-smoke-run.md`](llm-consumer-smoke-run.md).

## `AllenBufferedTopologyManyRelationsTest`

```
=== Buffered Allen relations (13 + multi-buffer) ===
r-BEFORE → BEFORE
r-MEETS → MEETS
r-OVERLAPS → OVERLAPS
r-FINISHED_BY → FINISHED_BY
r-CONTAINS → CONTAINS
r-STARTS → STARTS
r-EQUALS → EQUALS
r-STARTED_BY → STARTED_BY
r-DURING → DURING
r-FINISHES → FINISHES
r-OVERLAPPED_BY → OVERLAPPED_BY
r-MET_BY → MET_BY
r-AFTER → AFTER
r-MULTI-BUFFER:
  BEFORE  a={"startMs":0,"endMs":100}  b={"startMs":250,"endMs":280}
  CONTAINS  a={"startMs":200,"endMs":300}  b={"startMs":250,"endMs":280}
========================================================
```

## `AllenBufferedTopologyDemoTest` (join vs buffered on the same inputs)

```
=== Allen intervals demo (TopologyTestDriver) ===
Input A: [0, 1000000]  Input B: [999000, 1000001]  (starts ~999000 ms apart, intervals overlap)
Join output (allen-inferences): (no records)
Buffered output (allen-inferences-buffered): {"a":{"endMs":1000000,"startMs":0},"b":{"endMs":1000001,"startMs":999000},"relation":"OVERLAPS","key":"room-1"}
===============================================
```

Note: JSON key order in the buffered line may vary between runs / Jackson serialization.
