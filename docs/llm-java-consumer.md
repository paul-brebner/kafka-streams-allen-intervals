# Java consumer: Allen inference JSON → Ollama

The Streams topologies in the parent project **produce** inference records. This **`llm-consumer`** module is a **separate process** that **consumes** those records and calls a **local** LLM (Ollama) over HTTP—keeping slow, variable work **out of the stream task threads**.

See **[`llm-consumer/README.md`](../llm-consumer/README.md)** for build/run commands and system properties.

**Flow:** `allen-inferences-buffered` (or `kafka.topic`) → parallel Ollama calls per poll batch → logs; optionally **`kafka.output.topic`** for JSON envelopes `{ inferenceJson, explanation, ... }`.

**Captured run:** [`docs/llm-consumer-smoke-run.md`](llm-consumer-smoke-run.md) — topic setup, `java -jar` command, and **example LLM log output** (two inferences, `llama3:latest`).

**Production note:** batch-then-commit is a simple demo; for strict at-least-once narration you would typically use **per-record error handling**, **retries**, **DLQ**, and/or **smaller batches** with back-pressure (pause partitions when Ollama is saturated).
