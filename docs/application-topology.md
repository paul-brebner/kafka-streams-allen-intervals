# Application topology (Kafka Streams + LLM consumer)

End-to-end view: **two interval input topics**, a **Kafka Streams** application that can run **both** inference paths (join-window and buffered—often compared in tests), **output topics** with JSON inferences, and a **separate** **`llm-consumer`** process that calls **Ollama**. Unit tests use **`TopologyTestDriver`** instead of a real cluster (no broker).

## Diagram (Mermaid)

Paste into any Mermaid-capable viewer (GitHub renders it on file view).

```mermaid
flowchart TB
  subgraph inputs[Kafka input topics]
    TA["intervals-a (IntervalRecord JSON)"]
    TB["intervals-b (IntervalRecord JSON)"]
  end

  subgraph ks[Kafka Streams application]
    subgraph joinPath[AllenInferenceTopology]
      KA1[KStream from intervals-a]
      KB1[KStream from intervals-b]
      JW[KStream-KStream join + JoinWindows 5m symmetric]
      VJ[ValueJoiner: AllenIntervalAlgebra to JSON bytes]
      KA1 --> JW
      KB1 --> JW
      JW --> VJ
    end

    subgraph bufPath[AllenBufferedTopology]
      KA2[KStream from intervals-a]
      KB2[KStream from intervals-b]
      TAG[mapValues: TaggedInterval A or B]
      MG[merge]
      FT[flatTransform: AllenBufferTransformer]
      ST[(KeyValueStore: allen-interval-buffers)]
      AL[AllenIntervalAlgebra vs opposite buffer]
      KA2 --> TAG
      KB2 --> TAG
      TAG --> MG --> FT
      FT <--> ST
      FT --> AL
    end
  end

  subgraph outputs[Kafka output topics]
    T1["allen-inferences (join path)"]
    T2["allen-inferences-buffered (buffered path)"]
  end

  subgraph llm[llm-consumer separate JVM]
    KC[KafkaConsumer manual commit after batch]
    OC[OllamaClient HTTP POST api/generate]
    OLL[Ollama local LLM e.g. llama3:latest]
    LOG[stdout logs]
    TEX[optional sink: kafka.output.topic]
  end

  subgraph py[Optional scripts/explain_inference_ollama.py]
    STD[stdin or JSON file]
    PY[stdlib HTTP to Ollama]
  end

  TA --> KA1
  TB --> KB1
  TA --> KA2
  TB --> KB2

  VJ --> T1
  AL --> T2

  T2 --> KC
  KC --> OC
  OC --> OLL
  OC --> LOG
  OC --> TEX

  STD --> PY
  PY --> OLL
```

### Reading the diagram

| Path | What it does |
|------|----------------|
| **Join** | Pairs **A** and **B** records on the same key when **stream timestamps** fall within **`JoinWindows`**; emits exact Allen relation JSON to **`allen-inferences`**. |
| **Buffered** | Tags side A/B, **merges**, **`flatTransform`** reads/writes **`allen-interval-buffers`**, compares each arrival to the **opposite** buffer, emits 0..n JSON rows to **`allen-inferences-buffered`**. |
| **`llm-consumer`** | Consumes **`allen-inferences-buffered`** (configurable), calls **Ollama** per record batch, logs explanations; may produce an **optional** sink topic. |
| **Python script** | Same Ollama API for **ad hoc** single JSON objects (not subscribed to Kafka unless you pipe from a consumer). |

Both Streams subgraphs **read the same two input topics**. In a combined topology (for example `AllenBufferedTopologyDemoTest`) each path has its own `stream()` source nodes toward the same topic names.

## Simplified dataflow

```mermaid
flowchart LR
  P[Producers or tests] --> TA[intervals-a]
  P --> TB[intervals-b]
  TA --> KS[Kafka Streams]
  TB --> KS
  KS --> J[allen-inferences]
  KS --> B[allen-inferences-buffered]
  B --> C[llm-consumer]
  C --> O[Ollama]
```

## Related code

| Piece | Entry / constants |
|-------|-------------------|
| Join topology | `AllenInferenceTopology` — `TOPIC_A`, `TOPIC_B`, `TOPIC_OUT`, `JOIN_HALF_WIDTH` |
| Buffered topology | `AllenBufferedTopology` — `TOPIC_OUT`, `BUFFER_STORE`, `MAX_PER_SIDE` |
| Transformer | `AllenBufferTransformer` |
| Composition table | `AllenComposition` — [`docs/allen-composition.md`](allen-composition.md) |
| LLM sidecar | `llm-consumer/…/LlmInferenceConsumerApp`, `OllamaClient` |
| Smoke run | [`llm-consumer-smoke-run.md`](llm-consumer-smoke-run.md) |
