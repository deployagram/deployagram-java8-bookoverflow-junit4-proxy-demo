package com.deployagram.demo.bookoverflow;

import com.deployagram.demo.bookoverflow.avro.BookSuggestionMessage;
import com.deployagram.demo.bookoverflow.avro.DontForgetMessage;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;

import static com.deployagram.demo.bookoverflow.BookOverflowEndpoints.BOOK_OVERFLOW_PORT;
import static com.deployagram.demo.bookoverflow.BookOverflowEndpoints.BOOK_OVERFLOW_SUGGESTION_PATH;
import static com.deployagram.demo.bookoverflow.BookOverflowEndpoints.CLASSIC_BOOKS_PORT;
import static com.deployagram.demo.bookoverflow.BookOverflowEndpoints.CLASSIC_BOOKS_RANDOM_PATH;
import static com.deployagram.demo.bookoverflow.BookOverflowEndpoints.SUGGESTIONS_TOPIC;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BookOverflowServiceTest {

    private static final String EMAIL_ADDRESS = "test@deployagram.com";

    private static final String CLASSIC_BOOK = "{\n"
            + "  \"name\": \"Refactoring\",\n"
            + "  \"edition\": \"1st\",\n"
            + "  \"format\": \"paperback\",\n"
            + "  \"authors\": [\n"
            + "    \"Fowler Martin\",\n"
            + "    \"Beck Kent\"\n"
            + "  ]\n"
            + "}\n";

    private static KafkaSchemaRegistry kafka;
    private static BookOverflowServer bookOverflow;
    private static KafkaConsumer<String, DontForgetMessage> suggestions;

    @Rule
    public WireMockRule classicBooks = new WireMockRule(CLASSIC_BOOKS_PORT);

    private Response response;

    @BeforeClass
    public static void startKafkaAndBookOverflow() throws IOException {
        kafka = new KafkaSchemaRegistry();
        kafka.start();
        bookOverflow = new BookOverflowServer(BOOK_OVERFLOW_PORT, "http://localhost:" + CLASSIC_BOOKS_PORT, kafka.suggestionsProducer());
        bookOverflow.start();
        suggestions = kafka.newSuggestionsConsumer();
        suggestions.subscribe(Collections.singletonList(SUGGESTIONS_TOPIC));
    }

    @AfterClass
    public static void stopBookOverflowAndKafka() {
        bookOverflow.stop();
        suggestions.close();
        kafka.stop();
    }

    @Test
    public void suggestsTheBookFromClassicBooksAndPublishesIt() throws IOException {
        givenClassicBooksHasABookToSuggest();

        whenTheSuggestionIsRequestedForAnEmailAddress();

        thenTheBookIsReturned();
        thenTheSuggestionIsPublished();
    }

    @Test
    public void rejectsARequestWithoutAnEmailAddress() throws IOException {
        whenTheSuggestionIsRequestedWithoutAnEmailAddress();

        thenTheRequestIsRejectedAsBadRequest();
    }

    private void givenClassicBooksHasABookToSuggest() {
        classicBooks.stubFor(get(urlEqualTo(CLASSIC_BOOKS_RANDOM_PATH))
                .withHeader("Accept", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(CLASSIC_BOOK)));
    }

    private void whenTheSuggestionIsRequestedForAnEmailAddress() throws IOException {
        response = call(BOOK_OVERFLOW_PORT, BOOK_OVERFLOW_SUGGESTION_PATH + "?email=" + EMAIL_ADDRESS);
    }

    private void whenTheSuggestionIsRequestedWithoutAnEmailAddress() throws IOException {
        response = call(BOOK_OVERFLOW_PORT, BOOK_OVERFLOW_SUGGESTION_PATH);
    }

    private void thenTheBookIsReturned() {
        assertEquals(200, response.status);
        assertEquals(CLASSIC_BOOK, response.body);
    }

    private void thenTheSuggestionIsPublished() {
        DontForgetMessage published = awaitSuggestion();
        assertEquals(EMAIL_ADDRESS, published.getEmail().toString());
        BookSuggestionMessage suggestion = published.getSuggestion();
        assertEquals("Refactoring", suggestion.getName().toString());
        assertEquals("1st", suggestion.getEdition().toString());
        assertEquals("paperback", suggestion.getFormat().toString());
    }

    private void thenTheRequestIsRejectedAsBadRequest() {
        assertEquals(400, response.status);
    }

    private static DontForgetMessage awaitSuggestion() {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, DontForgetMessage> record : suggestions.poll(Duration.ofMillis(500))) {
                return record.value();
            }
        }
        fail("No suggestion was published to " + SUGGESTIONS_TOPIC + " within 10 seconds");
        return null;
    }

    private static Response call(int port, String pathAndQuery) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:" + port + pathAndQuery).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/json");
        int status = connection.getResponseCode();
        InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
        String body = stream == null ? "" : read(stream);
        connection.disconnect();
        return new Response(status, body);
    }

    private static String read(InputStream stream) throws IOException {
        StringBuilder text = new StringBuilder();
        try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            char[] buffer = new char[1024];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                text.append(buffer, 0, read);
            }
        }
        return text.toString();
    }

    private static final class Response {
        final int status;
        final String body;

        Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
