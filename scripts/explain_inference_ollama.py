#!/usr/bin/env python3
"""
Turn one Kafka Streams / Allen inference JSON object into a short natural-language explanation,
using a **local** model served by Ollama (default: http://127.0.0.1:11434).

Prerequisites:
  - Install Ollama: https://ollama.com
  - `ollama pull llama3:latest` (or set `--model` / env `OLLAMA_MODEL` to a tag you have)

Usage:
  python3 scripts/explain_inference_ollama.py scripts/example-inference.json
  cat scripts/example-inference.json | python3 scripts/explain_inference_ollama.py
  python3 scripts/explain_inference_ollama.py --dry-run scripts/example-inference.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


SYSTEM = """You help site reliability engineers. You receive ONE JSON object emitted by a streaming \
pipeline. Fields: "key" (entity id), "relation" (one of Allen's thirteen interval relations between \
closed intervals a and b), "a" and "b" with "startMs" and "endMs". Explain in 2-4 short sentences what \
this means in operational language. Do not re-derive the relation with arithmetic unless checking; \
trust the relation label. Do not use markdown headings."""


def read_inference_json(path: str | None) -> dict:
    if path is None or path == "-":
        raw = sys.stdin.read()
    else:
        with open(path, encoding="utf-8") as f:
            raw = f.read()
    return json.loads(raw)


def ollama_generate(host: str, model: str, user_prompt: str) -> str:
    url = host.rstrip("/") + "/api/generate"
    body = json.dumps(
        {
            "model": model,
            "prompt": user_prompt,
            "system": SYSTEM,
            "stream": False,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    text = payload.get("response")
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError(f"Unexpected Ollama response: {payload}")
    return text.strip()


def main() -> int:
    p = argparse.ArgumentParser(description="Explain Allen inference JSON via local Ollama model.")
    p.add_argument(
        "json_file",
        nargs="?",
        default=None,
        help="Path to JSON file (omit or use '-' for stdin)",
    )
    p.add_argument(
        "--model",
        default=os.environ.get("OLLAMA_MODEL", "llama3:latest"),
        help="Ollama model name (default: env OLLAMA_MODEL or llama3:latest)",
    )
    p.add_argument("--host", default="http://127.0.0.1:11434", help="Ollama base URL")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the user prompt only; do not call Ollama",
    )
    args = p.parse_args()

    try:
        inference = read_inference_json(args.json_file)
    except json.JSONDecodeError as e:
        print("Invalid JSON:", e, file=sys.stderr)
        return 2

    user_prompt = (
        "Here is the JSON inference object:\n\n"
        + json.dumps(inference, indent=2)
        + "\n\nExplain it for an on-call engineer."
    )

    if args.dry_run:
        print("--- system ---\n", SYSTEM, "\n--- user ---\n", user_prompt, sep="")
        return 0

    try:
        out = ollama_generate(args.host, args.model, user_prompt)
    except urllib.error.URLError as e:
        print(
            "Could not reach Ollama at",
            args.host,
            "\nStart Ollama and ensure the model is pulled (`ollama pull",
            args.model + "`).",
            "\nUnderlying error:",
            e,
            file=sys.stderr,
        )
        return 1
    except Exception as e:
        print("Error:", e, file=sys.stderr)
        return 1

    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
