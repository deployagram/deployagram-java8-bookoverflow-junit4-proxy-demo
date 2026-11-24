package com.deployagram.demo.bookoverflow;

/**
 * The ports, paths and topic the BookOverflow tests use. Shared by every test class so the tests agree on one place.
 */
final class BookOverflowEndpoints {

    static final int BOOK_OVERFLOW_PORT = 8081;
    static final int CLASSIC_BOOKS_PORT = 9050;

    static final String BOOK_OVERFLOW_SUGGESTION_PATH = "/BookOverflow/suggestion";
    static final String CLASSIC_BOOKS_RANDOM_PATH = "/ClassicBooks/random";

    static final String SUGGESTIONS_TOPIC = "SuppliedSuggestionsTopic";

    private BookOverflowEndpoints() {
    }
}
