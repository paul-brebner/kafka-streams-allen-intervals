package com.example.allenstreams.llm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone consumer: reads Allen inference JSON (same shape as {@code IntervalSerde.inferenceToJsonBytes}),
 * calls a local Ollama model in parallel for each <em>poll batch</em>, then commits offsets only after the
 * whole batch succeeds (demo-oriented; tune for production).
 *
 * <p>Configuration (system properties or {@code -Dkey=value}):
 *
 * <ul>
 *   <li>{@code kafka.bootstrap.servers} — default {@code localhost:9092}
 *   <li>{@code kafka.group.id} — default {@code inference-llm-consumer}
 *   <li>{@code kafka.topic} — default {@code allen-inferences-buffered}
 *   <li>{@code kafka.output.topic} — optional; if set, JSON explanations are produced here (String value)
 *   <li>{@code ollama.base.url} — default {@code http://127.0.0.1:11434}
 *   <li>{@code ollama.model} — default {@code llama3:latest} (override with env {@code OLLAMA_MODEL} in shell
 *       before {@code java -jar ...} if desired)
 *   <li>{@code ollama.request.timeout.seconds} — default {@code 120}
 *   <li>{@code llm.worker.threads} — default {@code 4}
 * </ul>
 */
public final class LlmInferenceConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(LlmInferenceConsumerApp.class);

    public static void main(String[] args) {
        String bootstrap = prop("kafka.bootstrap.servers", "localhost:9092");
        String groupId = prop("kafka.group.id", "inference-llm-consumer");
        String topic = prop("kafka.topic", "allen-inferences-buffered");
        String outputTopic = System.getProperty("kafka.output.topic", "").strip();
        String ollamaBase = prop("ollama.base.url", "http://127.0.0.1:11434");
        String model = prop("ollama.model", System.getenv().getOrDefault("OLLAMA_MODEL", "llama3:latest"));
        int timeoutSec = Integer.parseInt(prop("ollama.request.timeout.seconds", "120"));
        int workers = Integer.parseInt(prop("llm.worker.threads", "4"));

        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        OllamaClient ollama = new OllamaClient(ollamaBase, model, Duration.ofSeconds(timeoutSec));
        ObjectMapper mapper = new ObjectMapper();
        ExecutorService pool = Executors.newFixedThreadPool(workers);

        final KafkaProducer<String, String> kafkaProducer;
        if (!outputTopic.isEmpty()) {
            Properties pprops = new Properties();
            pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            pprops.put(ProducerConfig.ACKS_CONFIG, "all");
            kafkaProducer = new KafkaProducer<>(pprops);
            log.info("Explanations will be sent to topic {}", outputTopic);
        } else {
            kafkaProducer = null;
        }

        log.info(
                "Subscribing to topic={} group={} bootstrap={} ollama={} model={} workers={}",
                topic,
                groupId,
                bootstrap,
                ollamaBase,
                model,
                workers);

        AtomicBoolean running = new AtomicBoolean(true);
        final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(cprops);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    log.info("Shutdown requested");
                                    running.set(false);
                                    consumer.wakeup();
                                },
                                "llm-consumer-shutdown"));

        consumer.subscribe(List.of(topic));
        try {
            while (running.get()) {
                ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                } catch (WakeupException e) {
                    if (!running.get()) {
                        break;
                    }
                    throw e;
                }
                if (records.isEmpty()) {
                    continue;
                }
                List<ConsumerRecord<String, byte[]>> list = new ArrayList<>();
                records.forEach(list::add);

                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (ConsumerRecord<String, byte[]> r : list) {
                    futures.add(
                            CompletableFuture.runAsync(
                                    () -> {
                                        try {
                                            processOne(r, ollama, mapper, kafkaProducer, outputTopic);
                                        } catch (Exception e) {
                                            throw new RuntimeException(
                                                    "LLM failed for "
                                                            + r.topic()
                                                            + "-"
                                                            + r.partition()
                                                            + "@"
                                                            + r.offset(),
                                                    e);
                                        }
                                    },
                                    pool));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, byte[]> r : list) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    long next = r.offset() + 1;
                    offsets.merge(
                            tp,
                            new OffsetAndMetadata(next),
                            (a, b) -> a.offset() >= b.offset() ? a : b);
                }
                consumer.commitSync(offsets);
                log.debug("Committed {} partition offsets for batch of {}", offsets.size(), list.size());
            }
        } finally {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
            consumer.close(Duration.ofSeconds(10));
            if (kafkaProducer != null) {
                kafkaProducer.flush();
                kafkaProducer.close(Duration.ofSeconds(10));
            }
        }
    }

    private static void processOne(
            ConsumerRecord<String, byte[]> r,
            OllamaClient ollama,
            ObjectMapper mapper,
            KafkaProducer<String, String> producer,
            String outputTopic)
            throws Exception {
        String json = new String(r.value(), StandardCharsets.UTF_8);
        String explanation = ollama.explainInferenceJson(json);
        log.info(
                "partition={} offset={} key={}\n---\n{}\n---",
                r.partition(),
                r.offset(),
                r.key(),
                explanation);
        if (producer != null && !outputTopic.isEmpty()) {
            ObjectNode envelope =
                    mapper.createObjectNode()
                            .put("sourceTopic", r.topic())
                            .put("sourcePartition", r.partition())
                            .put("sourceOffset", r.offset())
                            .put("key", r.key() == null ? "" : r.key())
                            .put("inferenceJson", json)
                            .put("explanation", explanation);
            producer.send(new ProducerRecord<>(outputTopic, r.key(), mapper.writeValueAsString(envelope)));
        }
    }

    private static String prop(String key, String def) {
        String v = System.getProperty(key);
        return v == null || v.isBlank() ? def : v.strip();
    }

    private LlmInferenceConsumerApp() {}
}
