/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.flatbuffers.FlexBuffersBuilder;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** {@link FlatBuffersSerializer} 动态（FlexBuffers）往返测试。 */
class FlatBuffersSerializerTest {

    private final MessageSerializer<Sample> serializer = new FlatBuffersSerializer<>();
    private final MessageSerializer<String> stringSerializer = new FlatBuffersSerializer<>();
    private final MessageSerializer<byte[]> byteArraySerializer = new FlatBuffersSerializer<>();
    private final MessageSerializer<Map> mapSerializer = new FlatBuffersSerializer<>();

    @Test
    void roundTripPojo() {
        Sample src = sample();
        byte[] bytes = serializer.serialize(src, Sample.class);
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
        String src = "hello-flatbuffers-中文";
        byte[] bytes = stringSerializer.serialize(src, String.class);
        assertEquals(src, stringSerializer.deserialize(bytes, String.class));
    }

    @Test
    void roundTripByteArray() {
        byte[] src = new byte[] {1, 2, 3, -1, -128, 127};
        byte[] bytes = byteArraySerializer.serialize(src, byte[].class);
        assertArrayEquals(src, byteArraySerializer.deserialize(bytes, byte[].class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void roundTripNestedCollectionAndMap() {
        Map<String, List<Long>> nested = new LinkedHashMap<>();
        nested.put("a", Arrays.asList(1L, 2L, 3L));
        nested.put("b", Arrays.asList(4L, 5L));
        byte[] bytes = mapSerializer.serialize(nested, Map.class);
        Map<String, List<Long>> out = mapSerializer.deserialize(bytes, Map.class);
        assertEquals(nested, out);
    }

    @Test
    void nullAndEmptyReturnNull() {
        assertNull(serializer.serialize(null, Sample.class));
        assertNull(serializer.deserialize(null, Sample.class));
        assertNull(serializer.deserialize(new byte[0], Sample.class));
    }

    // ============================================================
    // C-06 类型映射：Map 键必须为 String；集合/映射按声明类型回塑
    // ============================================================

    @Test
    @SuppressWarnings("unchecked")
    void nonStringMapKeyFailsFastOnSerialize() {
        Map<Integer, String> numericKeyMap = new LinkedHashMap<>();
        numericKeyMap.put(1, "one");
        // 旧实现用 String.valueOf(key) 静默变形，读回后 key 变 String（map.get(1) 恒 null）
        SerializationException ex =
                assertThrows(
                        SerializationException.class,
                        () -> mapSerializer.serialize(numericKeyMap, Map.class));
        assertTrue(ex.getMessage().contains("String map keys"), ex.getMessage());

        Map<String, String> nullKeyMap = new LinkedHashMap<>();
        nullKeyMap.put(null, "v");
        assertThrows(
                SerializationException.class, () -> mapSerializer.serialize(nullKeyMap, Map.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void typedCollectionAndMapFieldsAreRebuiltByDeclaredType() {
        TypedHolder src = new TypedHolder();
        src.list = new ArrayList<>(Arrays.asList("a", "b", "a"));
        src.set = new LinkedHashSet<>(Arrays.asList("x", "y"));
        src.sortedSet = new TreeSet<>(Arrays.asList("m", "a", "z"));
        src.deque = new ArrayDeque<>(Arrays.asList("first", "second"));
        src.map = new LinkedHashMap<>(Map.of("k1", 1, "k2", 2));
        src.sortedMap = new TreeMap<>(Map.of("b", 2, "a", 1));

        MessageSerializer<TypedHolder> holderSerializer = new FlatBuffersSerializer<>();
        TypedHolder out =
                holderSerializer.deserialize(
                        holderSerializer.serialize(src, TypedHolder.class), TypedHolder.class);

        // 旧实现一律塞 ArrayList/LinkedHashMap：Set/Deque/SortedMap 字段会 IllegalArgumentException
        assertInstanceOf(ArrayList.class, out.list);
        assertInstanceOf(LinkedHashSet.class, out.set);
        assertInstanceOf(TreeSet.class, out.sortedSet);
        assertInstanceOf(ArrayDeque.class, out.deque);
        assertInstanceOf(LinkedHashMap.class, out.map);
        assertInstanceOf(TreeMap.class, out.sortedMap);

        assertEquals(src.list, out.list);
        assertEquals(src.set, out.set);
        assertEquals(src.sortedSet, out.sortedSet);
        assertEquals(new ArrayList<>(src.deque), new ArrayList<>(out.deque));
        assertEquals(src.map, out.map);
        assertEquals(src.sortedMap, out.sortedMap);
    }

    // ============================================================
    // C-04 / C-10 畸形字节与放大攻击边界：一律抛 SerializationException
    // ============================================================

    @Test
    void malformedAndGarbageBytesAreRejected() {
        // 垃圾字节：根引用类型为 INT（非 map），必须显式拒绝而不是静默返回 null / 裸异常
        byte[] garbage = {1, 2, 3, 4, 5, 6, 7, 8};
        assertThrows(
                SerializationException.class, () -> mapSerializer.deserialize(garbage, Map.class));

        // 截断的合法载荷 + 根类型标记为标量：命中畸形载荷拒绝路径
        Map<String, String> src = new LinkedHashMap<>();
        src.put("k", "v");
        byte[] valid = mapSerializer.serialize(src, Map.class);
        byte[] truncated = Arrays.copyOf(valid, Math.max(1, valid.length - 3));
        truncated[truncated.length - 1] = 4; // packedType = INT（非 map）
        assertThrows(
                SerializationException.class,
                () -> mapSerializer.deserialize(truncated, Map.class));
    }

    @Test
    void flippedBlobLengthFieldIsRejectedWithoutHugeAllocation() {
        MessageSerializer<byte[]> blobSerializer = new FlatBuffersSerializer<>();
        byte[] blob = new byte[100_000];
        Arrays.fill(blob, (byte) 0x5A);
        byte[] payload = blobSerializer.serialize(blob, byte[].class);

        // FlexBuffers Blob 布局：[4 字节小端声明长度][数据]；定位数据起点后其前 4 字节即长度字段
        int dataStart = firstRunOf(payload, (byte) 0x5A, 8);
        assertTrue(dataStart >= 4, "应能在载荷中定位 Blob 数据区");
        int sizeField = dataStart - 4;
        assertEquals((byte) 0xA0, payload[sizeField], "Blob 声明长度应为 100000 的小端编码");
        assertEquals((byte) 0x86, payload[sizeField + 1]);
        assertEquals((byte) 0x01, payload[sizeField + 2]);
        assertEquals((byte) 0x00, payload[sizeField + 3]);

        // 翻转为 0x7FFFFFFF（约 2GB）：旧实现会先按声明长度分配数组
        payload[sizeField] = (byte) 0xFF;
        payload[sizeField + 1] = (byte) 0xFF;
        payload[sizeField + 2] = (byte) 0xFF;
        payload[sizeField + 3] = (byte) 0x7F;

        SerializationException ex =
                assertThrows(
                        SerializationException.class,
                        () -> blobSerializer.deserialize(payload, byte[].class));
        assertTrue(ex.getMessage().contains("declared"), ex.getMessage());
        assertTrue(ex.getMessage().contains("Blob"), ex.getMessage());
    }

    @Test
    void deeplyNestedPayloadIsRejectedWithoutStackOverflow() {
        byte[] payload = nestedMapPayload(5_000);
        SerializationException ex =
                assertThrows(
                        SerializationException.class,
                        () -> mapSerializer.deserialize(payload, Map.class));
        assertTrue(ex.getMessage().contains("nested deeper"), ex.getMessage());
    }

    @Test
    void cyclicObjectGraphIsRejectedOnSerialize() {
        Node node = new Node();
        node.setNext(node);
        MessageSerializer<Node> nodeSerializer = new FlatBuffersSerializer<>();
        SerializationException ex =
                assertThrows(
                        SerializationException.class,
                        () -> nodeSerializer.serialize(node, Node.class));
        assertTrue(ex.getMessage().contains("nested deeper"), ex.getMessage());
    }

    /** 用 FlexBuffersBuilder 直接构造 {@code depth} 层 {"v": {"v": ... {"leaf": 1}}} 嵌套载荷。 */
    private static byte[] nestedMapPayload(int depth) {
        FlexBuffersBuilder builder = new FlexBuffersBuilder();
        int root = builder.startMap();
        int[] frames = new int[depth];
        for (int i = 0; i < depth; i++) {
            frames[i] = builder.startMap();
        }
        builder.putInt("leaf", 1);
        for (int i = depth - 1; i >= 0; i--) {
            builder.endMap("v", frames[i]);
        }
        builder.endMap(null, root);
        ByteBuffer buffer = builder.finish();
        buffer.rewind();
        byte[] out = new byte[buffer.remaining()];
        buffer.get(out);
        return out;
    }

    /** 返回第一个连续 {@code count} 个字节均为 {@code value} 的起始下标，找不到返回 -1。 */
    private static int firstRunOf(byte[] haystack, byte value, int count) {
        int run = 0;
        for (int i = 0; i < haystack.length; i++) {
            run = haystack[i] == value ? run + 1 : 0;
            if (run >= count) {
                return i - count + 1;
            }
        }
        return -1;
    }

    /** 覆盖 List/Set/SortedSet/Deque/Map/SortedMap 声明类型的 POJO。 */
    public static class TypedHolder {
        private List<String> list;
        private Set<String> set;
        private SortedSet<String> sortedSet;
        private Deque<String> deque;
        private Map<String, Integer> map;
        private SortedMap<String, Integer> sortedMap;

        public List<String> getList() {
            return list;
        }

        public void setList(List<String> list) {
            this.list = list;
        }

        public Set<String> getSet() {
            return set;
        }

        public void setSet(Set<String> set) {
            this.set = set;
        }

        public SortedSet<String> getSortedSet() {
            return sortedSet;
        }

        public void setSortedSet(SortedSet<String> sortedSet) {
            this.sortedSet = sortedSet;
        }

        public Deque<String> getDeque() {
            return deque;
        }

        public void setDeque(Deque<String> deque) {
            this.deque = deque;
        }

        public Map<String, Integer> getMap() {
            return map;
        }

        public void setMap(Map<String, Integer> map) {
            this.map = map;
        }

        public SortedMap<String, Integer> getSortedMap() {
            return sortedMap;
        }

        public void setSortedMap(SortedMap<String, Integer> sortedMap) {
            this.sortedMap = sortedMap;
        }
    }

    /** 自引用对象（写侧循环引用）。 */
    public static class Node {
        private Node next;

        public Node getNext() {
            return next;
        }

        public void setNext(Node next) {
            this.next = next;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void preservesNullCollectionElementsAndMapValues() {
        // 集合中的 null 元素必须保留下标，Map 中 value 为 null 的条目不能整条丢失
        Map<String, Object> src = new LinkedHashMap<>();
        src.put("a", 1L);
        src.put("b", null);
        src.put("c", Arrays.asList("x", null, "z"));

        byte[] bytes = mapSerializer.serialize(src, Map.class);
        Map<String, Object> out = mapSerializer.deserialize(bytes, Map.class);

        assertEquals(3, out.size());
        assertEquals(1L, out.get("a"));
        assertNull(out.get("b"));
        assertEquals(Arrays.asList("x", null, "z"), out.get("c"));
    }

    private Sample sample() {
        Sample s = new Sample();
        s.name = "order-1";
        s.count = 7;
        s.ts = 1_700_000_000_000L;
        s.active = true;
        s.price = 19.99;
        s.amount = new BigDecimal("12345.6789");
        s.raw = new byte[] {9, 8, 7};
        s.tags = new ArrayList<>(Arrays.asList("a", "b", "c"));
        s.scores = new LinkedHashMap<>();
        s.scores.put("math", 100);
        s.scores.put("eng", 95);
        return s;
    }

    /** 任意 POJO（无注解、无接口），用于验证动态反射序列化。 */
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
