package com.deployagram.demo.bookoverflow;

import com.deployagram.demo.bookoverflow.avro.DontForgetMessage;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Kafka and the Confluent Schema Registry, in Testcontainers, on the Deployagram Docker network so the Collector can read
 * the suggestions back. The application publishes Avro messages to the suggestions topic, and the tests read them with
 * the Avro deserializer.
 */
final class KafkaSchemaRegistry {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.6.1");
    private static final DockerImageName SCHEMA_REGISTRY_IMAGE = DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1");

    private static final String DEPLOYAGRAM_NETWORK = "deployagram";
    private static final int KAFKA_COLLECTOR_PORT = 19092;
    private static final int SCHEMA_REGISTRY_PORT = 8081;

    private final String suffix = UUID.randomUUID().toString().substring(0, 8);
    private final String kafkaContainerName = "bookoverflow-kafka-" + suffix;
    private final String schemaRegistryContainerName = "bookoverflow-schema-registry-" + suffix;

    private KafkaContainer kafka;
    private GenericContainer<?> schemaRegistry;
    private KafkaProducer<String, DontForgetMessage> producer;

    void start() throws IOException {
        kafka = new KafkaContainer(KAFKA_IMAGE)
                .withNetworkMode(DEPLOYAGRAM_NETWORK)
                .withCreateContainerCmdModifier(command -> command.withName(kafkaContainerName))
                .withListener(() -> kafkaContainerName + ":" + KAFKA_COLLECTOR_PORT);
        schemaRegistry = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
                .withNetworkMode(DEPLOYAGRAM_NETWORK)
                .withCreateContainerCmdModifier(command -> command.withName(schemaRegistryContainerName))
                .withExposedPorts(SCHEMA_REGISTRY_PORT)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", schemaRegistryContainerName)
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + kafkaContainerName + ":" + KAFKA_COLLECTOR_PORT)
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(60)));
        kafka.start();
        schemaRegistry.start();

        // The Collector reads the suggestions back, so it needs the broker and the registry by their Docker names.
        DeployagramStack.registerKafkaWithCollector("PLAINTEXT://" + kafkaContainerName + ":" + KAFKA_COLLECTOR_PORT);
        DeployagramStack.registerSchemaRegistryWithCollector("http://" + schemaRegistryContainerName + ":" + SCHEMA_REGISTRY_PORT);
    }

    void stop() {
        if (producer != null) {
            producer.close();
        }
        if (schemaRegistry != null) {
            schemaRegistry.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }

    /** A producer for the application to publish suggestions with, reported to the Collector by the interceptor. */
    KafkaProducer<String, DontForgetMessage> suggestionsProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", schemaRegistryUrl());
        for (Map.Entry<String, Object> entry : DeployagramStack.kafkaProducerInterceptorConfig().entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        producer = new KafkaProducer<>(properties);
        return producer;
    }

    /** A consumer that reads every suggestion on the topic from the beginning. */
    KafkaConsumer<String, DontForgetMessage> newSuggestionsConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bookoverflow-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return new KafkaConsumer<>(properties);
    }

    private String schemaRegistryUrl() {
        return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}
