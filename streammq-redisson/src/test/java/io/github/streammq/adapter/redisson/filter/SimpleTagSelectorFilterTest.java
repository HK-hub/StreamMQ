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

class SimpleTagSelectorFilterTest {

    @Test
    void testSingleTagMatch() {
        SimpleTagSelectorFilter filter = new SimpleTagSelectorFilter("tag1");

        Message<String> message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag1").build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag2").build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testOrExpression() {
        SimpleTagSelectorFilter filter = new SimpleTagSelectorFilter("tag1 || tag2");

        Message<String> message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag1").build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag2").build();
        assertThat(filter.accept(message)).isTrue();

        message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag3").build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testAndExpression() {
        SimpleTagSelectorFilter filter = new SimpleTagSelectorFilter("tag1 && tag2");

        Message<String> message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag1").build();
        assertThat(filter.accept(message)).isFalse();

        message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag2").build();
        assertThat(filter.accept(message)).isFalse();
    }

    @Test
    void testWildcard() {
        SimpleTagSelectorFilter filter = new SimpleTagSelectorFilter("*");

        Message<String> message =
                MessageBuilder.<String>withPayload("test")
                        .topic("test-topic")
                        .tag("any-tag")
                        .build();
        assertThat(filter.accept(message)).isTrue();

        message = MessageBuilder.<String>withPayload("test").topic("test-topic").tag("").build();
        assertThat(filter.accept(message)).isTrue();
    }

    @Test
    void testEmptyExpression() {
        SimpleTagSelectorFilter filter = new SimpleTagSelectorFilter("");

        Message<String> message =
                MessageBuilder.<String>withPayload("test").topic("test-topic").tag("tag1").build();
        assertThat(filter.accept(message)).isTrue();

        filter = new SimpleTagSelectorFilter("   ");
        assertThat(filter.accept(message)).isTrue();
    }

    @Test
    void testOrder() {
        SimpleTagSelectorFilter filter = new SimpleTagSelectorFilter("tag1");
        assertThat(filter.order()).isEqualTo(-1);
    }

    @Test
    void testName() {
        SimpleTagSelectorFilter filter = new SimpleTagSelectorFilter("tag1");
        assertThat(filter.name()).isEqualTo("SimpleTagSelectorFilter");
    }
}
