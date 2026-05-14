# Local LLM layer (Ollama) on top of Allen JSON

The Kafka Streams topologies emit **small JSON objects** (`key`, `relation`, `a`, `b`). That shape is ideal input for a **local** language model: deterministic geometry stays in Java; the model adds **plain-language explanation** for humans.

This repo includes a **stdlib-only Python 3** helper:

- [`scripts/explain_inference_ollama.py`](../scripts/explain_inference_ollama.py) — POSTs to [Ollama](https://ollama.com)’s `/api/generate` with a fixed system prompt and your JSON as the user message.
- [`scripts/example-inference.json`](../scripts/example-inference.json) — sample payload matching `IntervalSerde.inferenceToJsonBytes` output.

## Prerequisites

1. Install and run **Ollama**.
2. Pull a model (example):

   ```bash
   ollama pull llama3:latest
   ```

## Usage

```bash
cd /Users/pbrebner/Applications/Experiments/kafka-streams-allen-intervals

# No network call: show the prompt that would be sent
python3 scripts/explain_inference_ollama.py --dry-run scripts/example-inference.json

# Call local Ollama (default model llama3:latest, default host http://127.0.0.1:11434)
python3 scripts/explain_inference_ollama.py scripts/example-inference.json

python3 scripts/explain_inference_ollama.py --model mistral scripts/example-inference.json
```

Pipe output from another tool:

```bash
echo '{"key":"k","relation":"BEFORE","a":{"startMs":0,"endMs":10},"b":{"startMs":20,"endMs":30}}' \
  | python3 scripts/explain_inference_ollama.py -
```

**Defaults:** Ollama model **`llama3:latest`** (override with env **`OLLAMA_MODEL`** or **`--model`**).

## Design note

For **continuous** narration from Kafka (not stdin), use the separate **Java consumer** in [`llm-consumer/`](../llm-consumer/) — see [`docs/llm-java-consumer.md`](llm-java-consumer.md) and a full smoke run with **saved LLM output** in [`docs/llm-consumer-smoke-run.md`](llm-consumer-smoke-run.md).

The model **must not** replace `AllenIntervalAlgebra` for classification in production. Use it **after** the engine has emitted a relation, for **narration, triage text, or tickets**—grounded in the JSON you already trust.
