# Java LLM consumer — smoke run (captured output)

End-to-end check: **Kafka** topic `allen-inferences-buffered` → **`llm-consumer`** JAR → **Ollama** (`llama3:latest`). This documents one successful run and the **LLM narration** that was logged.

## Prerequisites

- Kafka reachable at **`127.0.0.1:9092`** (or set `kafka.bootstrap.servers`).
- **Ollama** running at **`http://127.0.0.1:11434`** with **`llama3:latest`** pulled.
- Shaded JAR built: `llm-consumer/target/inference-llm-consumer-1.0-SNAPSHOT.jar` (`mvn -q package` in `llm-consumer/`).

## Reproduce (topic + sample messages)

Create the topic if needed (CLI paths depend on your install; below uses a **Docker** broker where scripts live under `/opt/kafka/bin/`—replace the container name with yours).

```bash
# Example: broker container from another local compose stack (adjust name/image).
docker exec news-freeform-kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic allen-inferences-buffered \
  --replication-factor 1 --partitions 1

# Two NDJSON lines (same shape as Streams inference JSON values)
cat scripts/sample-inferences-ndjson.txt | docker exec -i news-freeform-kafka-broker \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic allen-inferences-buffered
```

`kafka-console-producer` does **not** set the Kafka record **key** by default, so the consumer logs `key=null`. The JSON **payload** still includes `"key": "room-1"` / `"lane-2"` for the model.

## Run the consumer

Use a **fresh** consumer group once if you want to re-read the same messages from the start:

```bash
cd /Users/pbrebner/Applications/Experiments/kafka-streams-allen-intervals/llm-consumer

java -Dkafka.group.id="llm-smoke-$(date +%s)" \
  -Dkafka.bootstrap.servers=127.0.0.1:9092 \
  -Dkafka.topic=allen-inferences-buffered \
  -Dollama.model=llama3:latest \
  -Dllm.worker.threads=2 \
  -jar target/inference-llm-consumer-1.0-SNAPSHOT.jar
```

Stop with **Ctrl+C** after records are processed (shutdown hook calls `consumer.wakeup()`).

Latency is dominated by **Ollama** (often tens of seconds per batch on laptop CPUs).

---

## Captured log excerpt (this run)

Kafka consumer startup noise (config dump) is omitted below. These are the **`LlmInferenceConsumerApp`** lines that include the model text (between `---`).

```
[main] INFO com.example.allenstreams.llm.LlmInferenceConsumerApp - Subscribing to topic=allen-inferences-buffered group=llm-smoke-1778733963 bootstrap=127.0.0.1:9092 ollama=http://127.0.0.1:11434 model=llama3:latest workers=2
… (Kafka client join / assignment / offset reset omitted) …
[pool-1-thread-1] INFO com.example.allenstreams.llm.LlmInferenceConsumerApp - partition=0 offset=0 key=null
---
This means that there's a timing overlap between two events in "room-1". The event 'a' started at 0 milliseconds and ended at 100 milliseconds, while the event 'b' started at 50 milliseconds and ended at 150 milliseconds. In other words, 'b' happened entirely within or overlapped with 'a'.
---
[pool-1-thread-2] INFO com.example.allenstreams.llm.LlmInferenceConsumerApp - partition=0 offset=1 key=null
---
This event indicates that the entity with ID "lane-2" is expected to be in a state before another interval of time, which starts at 20 milliseconds and ends at 30 milliseconds. In other words, "lane-2" should have already finished its work by the time this other interval begins.
---
[llm-consumer-shutdown] INFO com.example.allenstreams.llm.LlmInferenceConsumerApp - Shutdown requested
```

### Notes

- **OVERLAPS** example: the relation label is authoritative; the phrase “entirely within” is **not** strictly correct for this pair (it is **overlap**, not **during**). That is why narration should be treated as **assistive**, not a second classifier.
- **BEFORE** example: prose is broadly consistent with interval **A** finishing before **B** starts.

---

## See also

- [`llm-consumer/README.md`](../llm-consumer/README.md) — build, system properties, optional output topic.
- [`docs/llm-java-consumer.md`](llm-java-consumer.md) — architecture blurb.
- [`scripts/sample-inferences-ndjson.txt`](../scripts/sample-inferences-ndjson.txt) — payloads used for this run.
