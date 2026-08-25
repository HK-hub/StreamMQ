/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.filter;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import org.junit.jupiter.api.Test;

class SimpleSqlSelectorFilterTest {

    @Test
    void testStringEquals() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("name = 'John'");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("name", "John")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("name", "Jane")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testNumericComparison() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("age >= 18");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("age", "20")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("age", "17")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testNotEqual() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("status != 'FAIL'");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("status", "SUCCESS")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("status", "FAIL")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testAndExpression() {
        SimpleSqlSelectorFilter filter =
                new SimpleSqlSelectorFilter("type = 'order' AND amount > 100");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("type", "order")
                        .withProperty("amount", "200")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("type", "order")
                        .withProperty("amount", "50")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testOrExpression() {
        SimpleSqlSelectorFilter filter =
                new SimpleSqlSelectorFilter("type = 'order' OR type = 'payment'");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("type", "order")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("type", "payment")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("type", "refund")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testNotExpression() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("NOT status = 'DELETED'");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("status", "ACTIVE")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("status", "DELETED")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testParentheses() {
        SimpleSqlSelectorFilter filter =
                new SimpleSqlSelectorFilter(
                        "(type = 'order' OR type = 'payment') AND amount > 100");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("type", "order")
                        .withProperty("amount", "150")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("type", "order")
                        .withProperty("amount", "50")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testIsNull() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("deleted IS NULL");

        Message<String> message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("deleted", "true")
                        .build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testIsNotNull() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("deleted IS NOT NULL");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("deleted", "true")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test").topic("test-topic").build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testEmptyExpression() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .withProperty("name", "John")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        filter = new SimpleSqlSelectorFilter("   ");
        assertThat(filter.accept(message)).isTrue();

        filter = new SimpleSqlSelectorFilter("*");
        assertThat(filter.accept(message)).isTrue();
    }

    @Test
    void testOrder() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("name = 'John'");
        assertThat(filter.order()).isEqualTo(-1);
    }

    @Test
    void testName() {
        SimpleSqlSelectorFilter filter = new SimpleSqlSelectorFilter("name = 'John'");
        assertThat(filter.name()).isEqualTo("SimpleSqlSelectorFilter");
    }
}
