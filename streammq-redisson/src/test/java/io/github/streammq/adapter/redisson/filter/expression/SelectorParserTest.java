package io.github.streammq.adapter.redisson.filter.expression;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SelectorParserTest {

    @Test
    void testSimpleEquality() {
        Expression expr = SelectorParser.build("a = 'b'");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "b")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "c")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testNumericComparison() {
        Expression expr = SelectorParser.build("age > 18");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("age", "20")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("age", "18")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testAndExpression() {
        Expression expr = SelectorParser.build("a = 'b' AND c = 'd'");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "b")
            .property("c", "d")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "b")
            .property("c", "e")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testOrExpression() {
        Expression expr = SelectorParser.build("a = 'b' OR c = 'd'");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "b")
            .property("c", "e")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "x")
            .property("c", "e")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testNotExpression() {
        Expression expr = SelectorParser.build("NOT a = 'b'");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "c")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "b")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testParentheses() {
        Expression expr = SelectorParser.build("(a = 'b' OR c = 'd') AND e = 'f'");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "b")
            .property("c", "x")
            .property("e", "f")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "b")
            .property("c", "x")
            .property("e", "x")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testIsNull() {
        Expression expr = SelectorParser.build("a IS NULL");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "value")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testIsNotNull() {
        Expression expr = SelectorParser.build("a IS NOT NULL");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("a", "value")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .build();
        assertThat(expr.evaluate(message)).isFalse();
    }

    @Test
    void testWordBoundary() {
        Expression expr = SelectorParser.build("ANDROID = 'test'");
        assertThat(expr).isNotNull();

        Message<String> message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("ANDROID", "test")
            .build();
        assertThat(expr.evaluate(message)).isTrue();

        expr = SelectorParser.build("ISLAND = 'test'");
        assertThat(expr).isNotNull();

        message = MessageBuilder.<String>withPayload("test")
            .topic("test-topic")
            .property("ISLAND", "test")
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