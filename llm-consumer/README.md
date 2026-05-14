# Java LLM consumer (`llm-consumer`)

Separate **Kafka consumer** that reads Allen inference messages (JSON bytes, same shape as `IntervalSerde.inferenceToJsonBytes`) and calls **Ollama** over HTTP. Processing within each **poll batch** is parallel (`ExecutorService`); offsets are committed **after the whole batch** completes (demo trade-off: slow batches block commits).

## Build

```bash
cd /Users/pbrebner/Applications/Experiments/kafka-streams-allen-intervals/llm-consumer
mvn -q package
```

Runnable JAR:

`target/inference-llm-consumer-1.0-SNAPSHOT.jar`

## Run (needs Kafka + Ollama)

```bash
java -jar target/inference-llm-consumer-1.0-SNAPSHOT.jar
```

Defaults: `localhost:9092`, topic `allen-inferences-buffered`, Ollama `http://127.0.0.1:11434`, model `llama3:latest`.

### System properties

| Property | Default |
|----------|---------|
| `kafka.bootstrap.servers` | `localhost:9092` |
| `kafka.group.id` | `inference-llm-consumer` |
| `kafka.topic` | `allen-inferences-buffered` |
| `kafka.output.topic` | *(empty — log only)* |
| `ollama.base.url` | `http://127.0.0.1:11434` |
| `ollama.model` | `llama3:latest` or env `OLLAMA_MODEL` |
| `ollama.request.timeout.seconds` | `120` |
| `llm.worker.threads` | `4` |

Example with optional sink topic:

```bash
java -Dkafka.output.topic=allen-inference-explanations \
  -jar target/inference-llm-consumer-1.0-SNAPSHOT.jar
```

Stop with **Ctrl+C** (shutdown hook wakes the consumer).

## Smoke run (Kafka + Ollama + captured LLM output)

Reproducible steps, sample NDJSON payloads, and **saved log excerpts** (two records → two Ollama explanations) live in the parent repo:

[`../docs/llm-consumer-smoke-run.md`](../docs/llm-consumer-smoke-run.md)

Sample messages to pipe to `kafka-console-producer`:

[`../scripts/sample-inferences-ndjson.txt`](../scripts/sample-inferences-ndjson.txt)

## Code layout

- `LlmInferenceConsumerApp` — poll loop, batch parallel LLM calls, commit.
- `OllamaClient` — `HttpClient` → `POST /api/generate` (non-streaming).
