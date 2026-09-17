/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.streammq.core.serializer.MessageSerializer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** {@link SbeSerializer} 信封模式往返测试（payload 内部由严格类型 Jackson 编码）。 */
class SbeSerializerTest {

    private final MessageSerializer<Sample> serializer = new SbeSerializer<>();
    private final MessageSerializer<String> stringSerializer = new SbeSerializer<>();
    private final MessageSerializer<byte[]> byteArraySerializer = new SbeSerializer<>();

    @Test
    void roundTripPojo() {
        Sample src = sample();
        byte[] bytes = serializer.serialize(src, Sample.class);
        assertTrue(SbeSerializer.looksLikeSbeEnvelope(bytes), "body 应封装在 SBE 信封中");
        assertTrue(bytes.length > 0);

        Sample out = serializer.deserialize(bytes, Sample.class);
        assertEquals(src.name, out.name);
        assertEquals(src.count, out.count);
        assertEquals(src.ts, out.ts);
        assertEquals(src.active, out.active);
        assertEquals(src.price, out.price, 0.0001);
        assertEquals(0, src.amount.compareTo(out.amount));
        assertArrayEquals(src.raw, out.raw);
        assertEquals(src.tags, out.tags);
        assertEquals(src.scores, out.scores);
    }

    @Test
    void roundTripString() {
        String src = "hello-sbe-中文";
        byte[] bytes = stringSerializer.serialize(src, String.class);
        assertTrue(SbeSerializer.looksLikeSbeEnvelope(bytes));
        assertEquals(src, stringSerializer.deserialize(bytes, String.class));
    }

    @Test
    void roundTripByteArray() {
        byte[] src = new byte[] {1, 2, 3, -1, -128, 127};
        byte[] bytes = byteArraySerializer.serialize(src, byte[].class);
        assertTrue(SbeSerializer.looksLikeSbeEnvelope(bytes));
        assertArrayEquals(src, byteArraySerializer.deserialize(bytes, byte[].class));
    }

    @Test
    void unchangedPayloadIsTransparent() {
        // 内层为 JSON，外层为 SBE 信封；反序列化应还原为与 Jackson 一致的 POJO
        Sample src = sample();
        byte[] bytes = serializer.serialize(src, Sample.class);
        Sample out = serializer.deserialize(bytes, Sample.class);
        assertNotNull(out);
        assertEquals(src.name, out.name);
    }

    @Test
    void nullReturnsNull() {
        assertNull(serializer.serialize(null, Sample.class));
        assertNull(serializer.deserialize(null, Sample.class));
    }

    private Sample sample() {
        Sample s = new Sample();
        s.name = "order-1";
        s.count = 7;
        s.ts = 1_700_000_000_000L;
        s.active = false;
        s.price = 19.99;
        s.amount = new BigDecimal("12345.6789");
        s.raw = new byte[] {9, 8, 7};
        s.tags = new ArrayList<>(Arrays.asList("a", "b", "c"));
        s.scores = new LinkedHashMap<>();
        s.scores.put("math", 100);
        s.scores.put("eng", 95);
        return s;
    }

    /** 任意 POJO（无注解、无接口），用于验证信封模式下对任意消息体的透明支持。 */
    public static class Sample {
        private String name;
        private int count;
        private long ts;
        private boolean active;
        private double price;
        private BigDecimal amount;
        private byte[] raw;
        private List<String> tags;
        private Map<String, Integer> scores;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public long getTs() {
            return ts;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public BigDecimal getAmount() {
            return amount;
        }

        public void setAmount(BigDecimal amount) {
            this.amount = amount;
        }

        public byte[] getRaw() {
            return raw;
        }

        public void setRaw(byte[] raw) {
            this.raw = raw;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public Map<String, Integer> getScores() {
            return scores;
        }

        public void setScores(Map<String, Integer> scores) {
            this.scores = scores;
        }
    }
}
