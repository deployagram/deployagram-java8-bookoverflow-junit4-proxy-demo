# deployagram-java8-bookoverflow-junit4-proxy-demo

BookOverflow, on Java 8, tested with JUnit 4, WireMock and Kafka with Avro. The HTTP calls go through the Deployagram HTTP
Proxy, and the Kafka suggestions are reported to the Deployagram Collector by a producer interceptor.

BookOverflow suggests a book for a reader. `GET /BookOverflow/suggestion?email=...` calls ClassicBooks over HTTP for a
random book, publishes the suggestion to `SuppliedSuggestionsTopic` as an Avro `DontForgetMessage`, and returns the book.
The message is the same one the Quarkus Avro demo publishes.

## Layout

- `src/main/avro/dont_forget_message.avsc`: the message schema. The Avro plugin generates the Java classes from it.
- `src/main/java/com/deployagram/demo/bookoverflow/BookOverflowServer.java`: the application. It uses the JDK's
  `HttpServer`, and it takes its Kafka producer as a constructor argument, so the tests supply it.
- `src/test/java/com/deployagram/demo/bookoverflow/BookOverflowServiceTest.java`: the JUnit 4 tests. ClassicBooks is a
  WireMock stub on port 9050, and the test reads the suggestion back from Kafka.
- `src/test/java/com/deployagram/demo/bookoverflow/KafkaSchemaRegistry.java`: Kafka and the Schema Registry, in
  Testcontainers, on the `deployagram` Docker network.
- `src/test/java/com/deployagram/demo/bookoverflow/DeployagramStack.java`: starts the Collector and the Proxy, posts the
  routes, tells the Collector where the broker and registry are, and sets the producer interceptor.

## Running the tests

You need Java 8, Maven, and Docker running, with the Deployagram images pulled.

Create two files in this folder. Both are listed in `.gitignore`. **Never commit them.**

`.env`

```
DEPLOYAGRAM_LICENSE_KEY=<your licence key>
DEPLOYAGRAM_CLOUD_USERNAME=<your Deployagram Cloud username>
DEPLOYAGRAM_CLOUD_PASSWORD=<your Deployagram Cloud password>
DEPLOYAGRAM_CLIENT_ID=<your Deployagram client ID>
```

`.deployagram` contains your licence file (the `-----BEGIN LICENSE FILE-----` block).

Then run the tests from the command line. The `set -a` line exports the variables in `.env` for the Maven run:

```bash
set -a; source .env; set +a
JAVA_HOME=/path/to/jdk-8 mvn -B test
```

From IntelliJ IDEA, the run configuration does not read `.env`. Add the same variables to the run configuration's
environment variables, or use the EnvFile plugin pointed at `.env`. If `DEPLOYAGRAM_LICENSE_KEY` is missing, the proxy
starts with a placeholder licence key, stops immediately, and the test fails with
`The Deployagram HTTP Proxy is not running`.

## Deployagram

The tests use `deployagram-junit4` (`DeployagramRule`), `deployagram-environment`, and `deployagram-kafka-interceptors`,
all from Maven Central.

- **Proxy.** BookOverflow calls ClassicBooks through the Proxy on port 9080, and the Proxy forwards to WireMock. The test
  also calls BookOverflow through the Proxy, so both sides of each HTTP call are recorded under the test's name.
- **Kafka.** `SchemaAwareProducerInterceptor` reports each acknowledged suggestion to the Collector. The Collector reads the
  record back from the broker and decodes it with the registry, so `KafkaSchemaRegistry` registers both with the Collector
  by their Docker names.
- **Collector.** The Collector is found or started by `DeployagramEnvironment`. Only the containers this run started are
  stopped at the end. A Collector that was already running is reused.
- **Local setup.** The licence file `.deployagram` and the `.env` file are described under "Running the tests". Neither is
  committed, and both are ignored by `.gitignore`.
- **Pipeline.** `.github/workflows/prod-release.yml` runs the tests on Java 8, then checks the contract before a Prod
  release. It needs the five repository secrets listed at the top of the workflow.
