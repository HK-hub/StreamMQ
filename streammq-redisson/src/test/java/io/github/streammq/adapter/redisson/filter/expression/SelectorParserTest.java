package io.github.streammq.adapter.redisson.filter.expression;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import org.junit.jupiter.api.Test;

class SelectorParserTest {

    @Test
    void testSimpleEquality() {
        Expression expr = SelectorParser.build("a = 'b'");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "b")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "c")
                        .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testNumericComparison() {
        Expression expr = SelectorParser.build("age > 18");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("age", "20")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("age", "18")
                        .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testAndExpression() {
        Expression expr = SelectorParser.build("a = 'b' AND c = 'd'");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "b")
                        .withProperty("c", "d")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "b")
                        .withProperty("c", "e")
                        .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testOrExpression() {
        Expression expr = SelectorParser.build("a = 'b' OR c = 'd'");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "b")
                        .withProperty("c", "e")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "x")
                        .withProperty("c", "e")
                        .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testNotExpression() {
        Expression expr = SelectorParser.build("NOT a = 'b'");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "c")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "b")
                        .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testParentheses() {
        Expression expr = SelectorParser.build("(a = 'b' OR c = 'd') AND e = 'f'");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "b")
                        .withProperty("c", "x")
                        .withProperty("e", "f")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "b")
                        .withProperty("c", "x")
                        .withProperty("e", "x")
                        .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testIsNull() {
        Expression expr = SelectorParser.build("a IS NULL");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").build();
        assertThat(expr.evaluate(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "value")
                        .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testIsNotNull() {
        Expression expr = SelectorParser.build("a IS NOT NULL");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("a", "value")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test").topic("test-topic").build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testWordBoundary() {
        Expression expr = SelectorParser.build("ANDROID = 'test'");
        assertThat(expr).isNotNull();

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("ANDROID", "test")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();

        expr = SelectorParser.build("ISLAND = 'test'");
        assertThat(expr).isNotNull();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("ISLAND", "test")
                        .build();
        assertThat(expr.evaluate(message)).isTrue();
    }

    @Test
    void testEmptyExpression() {
        assertThat(SelectorParser.build("")).isNull();
        assertThat(SelectorParser.build("   ")).isNull();
        assertThat(SelectorParser.build("*")).isNull();
    }

    @Test
    void testMalformedExpression() {
        Expression expr = SelectorParser.build("a = ");
        assertThat(expr).isNull();

        expr = SelectorParser.build("= 'b'");
        assertThat(expr).isNull();

        expr = SelectorParser.build("a >");
        assertThat(expr).isNull();
    }
}
