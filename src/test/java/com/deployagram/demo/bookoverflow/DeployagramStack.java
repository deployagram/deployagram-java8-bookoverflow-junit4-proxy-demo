package com.deployagram.demo.bookoverflow;

import com.deployagram.living.environment.DeployagramEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.deployagram.demo.bookoverflow.BookOverflowEndpoints.SUGGESTIONS_TOPIC;

/**
 * The Kafka side of the Deployagram setup: tells the Collector which broker and schema registry to read the suggestions
 * from, and configures the producer interceptor that reports each acknowledged suggestion.
 *
 * The Collector and the HTTP Proxy are started by {@code @Deployagram(startEnvironment = true)} on the test class, and the
 * HTTP routes are declared with {@code @DeployagramConfig} there, so this class does not start or configure them.
 */
final class DeployagramStack {

    private static final String APP_NAME = "BookOverflow";

    private DeployagramStack() {
    }

    /** The port the Proxy is listening on. Valid once the environment has started. */
    static int proxyPort() {
        return DeployagramEnvironment.getMappedPortForProxyIfRunning()
                .orElseThrow(() -> new IllegalStateException("The Deployagram HTTP Proxy is not running"));
    }

    static int collectorPort() {
        return DeployagramEnvironment.getMappedPortForLoggerIfRunning()
                .orElseThrow(() -> new IllegalStateException("The Deployagram Collector is not running"));
    }

    /** The producer interceptor reports each acknowledged suggestion to the Collector. */
    static Map<String, Object> kafkaProducerInterceptorConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("interceptor.classes", "com.deployagram.kafka.interceptors.SchemaAwareProducerInterceptor");
        config.put("logger.hostname", "localhost");
        config.put("logger.port", String.valueOf(collectorPort()));
        config.put("producing.app.name." + SUGGESTIONS_TOPIC, APP_NAME);
        return config;
    }

    /** Tells the Collector where the broker is, as seen from the Docker network. */
    static void registerKafkaWithCollector(String bootstrapServersForCollector) throws IOException {
        put("PUT", collectorUrl("/kafka/configuration"), singleton("bootstrapServers", bootstrapServersForCollector));
    }

    /** Tells the Collector where the schema registry is, as seen from the Docker network. */
    static void registerSchemaRegistryWithCollector(String schemaRegistryUrlForCollector) throws IOException {
        put("PUT", collectorUrl("/schemaRegistry/configuration"), singleton("url", schemaRegistryUrlForCollector));
    }

    private static String collectorUrl(String path) {
        return "http://localhost:" + collectorPort() + path;
    }

    private static Map<String, String> singleton(String key, String value) {
        Map<String, String> body = new LinkedHashMap<>();
        body.put(key, value);
        return body;
    }

    private static void put(String method, String url, Map<String, ?> body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestMethod(method);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);
        try (OutputStream out = connection.getOutputStream()) {
            out.write(toJson(body).getBytes(StandardCharsets.UTF_8));
        }
        int status = connection.getResponseCode();
        if (status >= 300) {
            InputStream error = connection.getErrorStream();
            throw new IOException(method + " " + url + " was refused: HTTP " + status + " " + (error == null ? "" : read(error)));
        }
        connection.disconnect();
    }

    private static String toJson(Map<String, ?> map) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            if (!first) {
                json.append(",");
            }
            json.append("\"").append(escape(entry.getKey())).append("\":\"").append(escape(String.valueOf(entry.getValue()))).append("\"");
            first = false;
        }
        return json.append("}").toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String read(InputStream in) throws IOException {
        try (InputStream source = in) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[4096];
            int read;
            while ((read = source.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
