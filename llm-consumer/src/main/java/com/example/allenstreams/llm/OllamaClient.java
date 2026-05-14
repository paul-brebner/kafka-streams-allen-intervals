package com.example.allenstreams.llm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Calls Ollama's <code>/api/generate</code> (non-streaming). */
public final class OllamaClient {

    private static final String SYSTEM =
            """
            You help site reliability engineers. You receive ONE JSON object emitted by a streaming \
            pipeline. Fields: "key" (entity id), "relation" (one of Allen's thirteen interval relations \
            between closed intervals a and b), "a" and "b" with "startMs" and "endMs". Explain in 2-4 short \
            sentences what this means in operational language. Trust the relation label. Do not use markdown \
            headings.""";

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String baseUrl;
    private final String model;
    private final Duration requestTimeout;

    public OllamaClient(String baseUrl, String model, Duration requestTimeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.model = model;
        this.requestTimeout = requestTimeout;
    }

    public String explainInferenceJson(String inferenceJson) throws IOException, InterruptedException {
        String userPrompt =
                "Here is the JSON inference object:\n\n" + inferenceJson + "\n\nExplain it for an on-call engineer.";
        ObjectNode body = mapper.createObjectNode();
        body.put("model", model);
        body.put("prompt", userPrompt);
        body.put("system", SYSTEM);
        body.put("stream", false);

        byte[] bytes = mapper.writeValueAsBytes(body);
        HttpRequest req =
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/generate"))
                        .timeout(requestTimeout)
                        .header("Content-Type", "application/json; charset=utf-8")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(bytes))
                        .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() != 200) {
            throw new IOException("Ollama HTTP " + resp.statusCode() + ": " + resp.body());
        }
        JsonNode root = mapper.readTree(resp.body());
        String text = root.path("response").asText(null);
        if (text == null || text.isBlank()) {
            throw new IOException("Unexpected Ollama response: " + resp.body());
        }
        return text.strip();
    }
}
