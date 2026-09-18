/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import com.google.flatbuffers.FlexBuffers;
import com.google.flatbuffers.FlexBuffersBuilder;
import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 FlatBuffers {@code FlexBuffers} 的<b>动态</b>内置序列化器（零拷贝读取，免 schema / 免代码生成）。
 *
 * <p>FlatBuffers 本身是 schema + 代码生成格式，但 {@code FlexBuffers} 是其 schema-less 的兄弟格式： 支持动态 Map / Vector
 * / 标量 / Blob，读取时<b>无需任何解析或拷贝</b>（直接基于 ByteBuffer 偏移量寻址）， 因此具备 FlatBuffers
 * 的核心优势（极快读、体积小、安全——纯数据、无反序列化代码执行面）。
 *
 * <p>与 {@code FurySerializer}/{@code ProtostuffSerializer} 一样，本序列化器通过反射处理<b>任意 POJO</b>，
 * 不要求业务消息体预注册或实现接口。顶层值统一包在一个根 Map 的 {@code "v"} 键下，便于标量 / 集合 / POJO 统一处理。
 *
 * <h3>类型映射</h3>
 *
 * <ul>
 *   <li>String / Boolean / 数值（含 BigDecimal/BigInteger，以字符串无损存储）/ byte[]（Blob）/ Collection / Map /
 *       POJO → 对应 FlexBuffers 类型；读取时按目标字段声明类型安全回塑。
 *   <li><b>集合 / 映射按声明类型构造：</b>{@code Set} → {@code LinkedHashSet}、{@code SortedSet} → {@code
 *       TreeSet}、{@code Deque} → {@code ArrayDeque}、{@code SortedMap} → {@code TreeMap}、 其余（{@code
 *       List}/{@code Collection}）→ {@code ArrayList}；声明为具体实现类时按其无参构造实例化。
 *   <li><b>Map 键仅支持 String：</b>FlexBuffers 的 map 键是字符串。非 String 键（含 null 键）在序列化时<b>快速失败</b>抛 {@link
 *       SerializationException}；旧实现用 {@code String.valueOf} 静默变形，会把 {@code Map<Integer,V>} 的键读回 为
 *       String（{@code map.get(1)} 恒为 null）。
 *   <li>不支持的类型（如无法实例化的抽象类、无无参构造的 POJO / 集合实现类）会抛出 {@link SerializationException}。
 * </ul>
 *
 * <p><b>⚠️ 安全：</b>FlexBuffers 内层格式是纯数据，不含类名、无 gadget 与多态反序列化面；但 SDK 外层仍会按目标类型 （消费者声明的泛型，或载荷 {@code
 * bodyType} 驱动的解析结果）反射物化对象，逐字段读取，集合 / Blob 会复制为堆对象。 读取侧对 Blob / 集合 / Map 的声明长度（单字段上限 {@link
 * #MAX_FIELD_LENGTH_BYTES} 字节）与嵌套深度（{@link #MAX_DEPTH} 层）做上限校验；畸形 / 深嵌套载荷统一抛 {@link
 * SerializationException}（含 {@link StackOverflowError} 兜底转换），
 * 不会按声明长度放大分配。写侧同样有深度上限，循环引用对象图会快速失败而不是无限递归。 选用本序列化器时，用户需在自身依赖中加入 {@code
 * com.google.flatbuffers:flatbuffers-java}（本模块以 optional 声明，不强制传递）。
 *
 * <p><b>null / 空输入契约：</b>{@code serialize(null)} 与 {@code deserialize(null|byte[0])} 均返回 {@code
 * null} （与其它内置序列化器一致）。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FlatBuffersSerializer<T> implements MessageSerializer<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FlatBuffersSerializer.class);

    /** 顶层值统一放在根 Map 的该键下。 */
    private static final String ROOT_KEY = "v";

    /**
     * 单个字段（Blob / String / 集合元素数 / Map 条目数）的声明长度上限： 取消息上限 {@link
     * StreamMQConstants#MAX_MESSAGE_SIZE_BYTES} 与 64MB 的较小值。
     *
     * <p>FlexBuffers 把长度声明写在不可信载荷里，若不校验，4 字节长度字段可被放大为一次 2GB 级分配（如 {@code Blob.getBytes()}
     * 先按声明长度分配数组）。
     */
    private static final int MAX_FIELD_LENGTH_BYTES =
            (int) Math.min(StreamMQConstants.MAX_MESSAGE_SIZE_BYTES, 64L * 1024 * 1024);

    /** 嵌套深度上限（读写对称）：读取侧防止深嵌套载荷栈溢出，写侧防止循环引用对象图无限递归。 */
    private static final int MAX_DEPTH = 64;

    @Override
    public byte[] serialize(T object, Class<T> type) throws SerializationException {
        if (object == null) {
            return null;
        }
        try {
            FlexBuffersBuilder builder = new FlexBuffersBuilder();
            int root = builder.startMap();
            writeValue(builder, ROOT_KEY, object, type, 0);
            builder.endMap(null, root);
            ByteBuffer buffer = builder.finish();
            buffer.rewind();
            byte[] out = new byte[buffer.remaining()];
            buffer.get(out);
            return out;
        } catch (SerializationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SerializationException(
                    "FlatBuffers serialize failed: " + ex.getMessage(), ex);
        }
    }

    @Override
    public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
        if (Objects.isNull(bytes) || bytes.length == 0) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            FlexBuffers.Reference root = FlexBuffers.getRoot(buffer);
            if (root.isNull()) {
                return null;
            }
            if (!root.isMap()) {
                // 本实现总是写入「根 Map + "v" 键」结构；根不是 map 说明载荷畸形/被篡改，
                // 必须显式拒绝而不是静默返回 null（null body 会被误认为合法的空消息）。
                throw new SerializationException(
                        "FlatBuffers deserialize failed: root value is not a FlexBuffers map"
                                + " (malformed payload)");
            }
            FlexBuffers.Reference value = root.asMap().get(ROOT_KEY);
            return readValue(value, type, 0);
        } catch (SerializationException ex) {
            throw ex;
        } catch (StackOverflowError ex) {
            // 兜底：即便未来 FlexBuffers 内部引入递归路径，深嵌套毒丸也不能以 Error 形态逃出消费路径
            LOG.warn(
                    "FlatBuffers deserialize rejected a payload that exhausted the stack"
                            + " (deeply nested structure); treating it as a malformed poison"
                            + " message");
            throw new SerializationException(
                    "FlatBuffers deserialize failed: nested structure exceeds the safe recursion"
                            + " depth");
        } catch (Exception ex) {
            throw new SerializationException(
                    "FlatBuffers deserialize failed: " + ex.getMessage(), ex);
        }
    }

    @Override
    public String name() {
        return "flatbuffers";
    }

    // ============================================================
    // 写入（序列化）：根据运行时类型将对象写入 FlexBuffersBuilder
    // ============================================================

    private void writeValue(
            FlexBuffersBuilder b, String key, Object value, Type genericType, int depth) {
        checkDepth(depth, "serialize");
        if (value == null) {
            // 保留 null 值（而非直接跳过 key），保证 Map 中 value 为 null 的条目可正确往返。
            b.putNull(key);
            return;
        }
        if (value instanceof String) {
            b.putString(key, (String) value);
        } else if (value instanceof Boolean) {
            b.putBoolean(key, (Boolean) value);
        } else if (value instanceof Character) {
            b.putInt(key, (int) (Character) value);
        } else if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            b.putInt(key, ((Number) value).intValue());
        } else if (value instanceof Long) {
            b.putInt(key, (Long) value);
        } else if (value instanceof Float) {
            b.putFloat(key, (Float) value);
        } else if (value instanceof Double) {
            b.putFloat(key, (Double) value);
        } else if (value instanceof BigDecimal) {
            b.putString(key, value.toString());
        } else if (value instanceof BigInteger) {
            b.putString(key, value.toString());
        } else if (value instanceof byte[]) {
            b.putBlob(key, (byte[]) value);
        } else if (value instanceof Collection) {
            writeCollection(b, key, (Collection<?>) value, genericType, depth);
        } else if (value instanceof Map) {
            writeMap(b, key, (Map<?, ?>) value, genericType, depth);
        } else if (value.getClass().isArray()) {
            writeArray(b, key, value, depth);
        } else {
            // 任意 POJO：递归写入其字段（深度 +1，循环引用对象图在 MAX_DEPTH 处快速失败）
            int m = b.startMap();
            writeFields(b, value, depth + 1);
            b.endMap(key, m);
        }
    }

    /** 写入集合元素（无 key，attach 到当前打开的 Vector）。 */
    private void writeElement(FlexBuffersBuilder b, Object value, Type elementType, int depth) {
        checkDepth(depth, "serialize");
        if (value == null) {
            // 向量中的 null 元素必须显式写入，否则会丢失该下标、导致反序列化后集合长度不一致。
            b.putNull();
            return;
        }
        if (value instanceof String) {
            b.putString((String) value);
        } else if (value instanceof Boolean) {
            b.putBoolean((Boolean) value);
        } else if (value instanceof Character) {
            b.putInt((int) (Character) value);
        } else if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            b.putInt(((Number) value).intValue());
        } else if (value instanceof Long) {
            b.putInt((Long) value);
        } else if (value instanceof Float) {
            b.putFloat((Float) value);
        } else if (value instanceof Double) {
            b.putFloat((Double) value);
        } else if (value instanceof BigDecimal) {
            b.putString(value.toString());
        } else if (value instanceof BigInteger) {
            b.putString(value.toString());
        } else if (value instanceof byte[]) {
            b.putBlob((byte[]) value);
        } else if (value instanceof Collection) {
            writeCollection(b, null, (Collection<?>) value, elementType, depth);
        } else if (value instanceof Map) {
            writeMap(b, null, (Map<?, ?>) value, elementType, depth);
        } else if (value.getClass().isArray()) {
            writeArray(b, null, value, depth);
        } else {
            int m = b.startMap();
            writeFields(b, value, depth + 1);
            b.endMap(null, m);
        }
    }

    private void writeCollection(
            FlexBuffersBuilder b, String key, Collection<?> value, Type genericType, int depth) {
        Type elementType = extractElementType(genericType);
        int v = b.startVector();
        for (Object e : value) {
            writeElement(b, e, elementType, depth + 1);
        }
        b.endVector(key, v, false, false);
    }

    private void writeMap(
            FlexBuffersBuilder b, String key, Map<?, ?> value, Type genericType, int depth) {
        Type valueType = extractMapValueType(genericType);
        int m = b.startMap();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            Object rawKey = entry.getKey();
            if (!(rawKey instanceof String)) {
                // FlexBuffers 的 map 键是字符串：非 String 键若用 String.valueOf 静默变形，
                // 读回后键类型漂移（Map<Integer,V> 变 Map<String,V>，map.get(1) 恒 null）。
                throw new SerializationException(
                        "FlatBuffers only supports String map keys, got key type "
                                + (Objects.isNull(rawKey) ? "null" : rawKey.getClass().getName())
                                + "; convert the map to Map<String, V> before serialization");
            }
            writeValue(b, (String) rawKey, entry.getValue(), valueType, depth + 1);
        }
        b.endMap(key, m);
    }

    private void writeArray(FlexBuffersBuilder b, String key, Object array, int depth) {
        int len = Array.getLength(array);
        int v = b.startVector();
        for (int i = 0; i < len; i++) {
            writeElement(b, Array.get(array, i), null, depth + 1);
        }
        b.endVector(key, v, false, false);
    }

    private void writeFields(FlexBuffersBuilder b, Object object, int depth) {
        Class<?> clazz = object.getClass();
        while (clazz != null && clazz != Object.class) {
            for (Field field : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    writeValue(
                            b, field.getName(), field.get(object), field.getGenericType(), depth);
                } catch (IllegalAccessException e) {
                    throw new SerializationException(
                            "FlatBuffers cannot access field "
                                    + field.getName()
                                    + ": "
                                    + e.getMessage(),
                            e);
                }
            }
            clazz = clazz.getSuperclass();
        }
    }

    // ============================================================
    // 读取（反序列化）：按目标类型从 FlexBuffers Reference 回塑
    // ============================================================

    @SuppressWarnings("unchecked")
    private <R> R readValue(FlexBuffers.Reference ref, Type type, int depth) {
        checkDepth(depth, "deserialize");
        if (ref == null || ref.isNull()) {
            return null;
        }
        Class<?> clazz = toClass(type);
        if (clazz == Object.class) {
            return (R) readDynamic(ref, depth);
        }
        if (clazz == String.class) {
            return (R) ref.asString();
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return (R) Boolean.valueOf(ref.asBoolean());
        } else if (clazz == Byte.class || clazz == byte.class) {
            return (R) Byte.valueOf((byte) ref.asLong());
        } else if (clazz == Short.class || clazz == short.class) {
            return (R) Short.valueOf((short) ref.asLong());
        } else if (clazz == Integer.class || clazz == int.class) {
            return (R) Integer.valueOf((int) ref.asLong());
        } else if (clazz == Long.class || clazz == long.class) {
            return (R) Long.valueOf(ref.asLong());
        } else if (clazz == Character.class || clazz == char.class) {
            return (R) Character.valueOf((char) ref.asLong());
        } else if (clazz == Float.class || clazz == float.class) {
            return (R) Float.valueOf((float) ref.asFloat());
        } else if (clazz == Double.class || clazz == double.class) {
            return (R) Double.valueOf(ref.asFloat());
        } else if (clazz == BigDecimal.class) {
            return (R) new BigDecimal(ref.asString());
        } else if (clazz == BigInteger.class) {
            return (R) new BigInteger(ref.asString());
        } else if (clazz == byte[].class) {
            return (R) readBlob(ref);
        } else if (Collection.class.isAssignableFrom(clazz)) {
            return (R) readCollection(ref, type, depth);
        } else if (Map.class.isAssignableFrom(clazz)) {
            return (R) readMap(ref, type, depth);
        } else {
            return (R) readPojo(ref, clazz, depth);
        }
    }

    /** 读取 Blob：先校验声明长度（超限抛异常），再按声明长度分配/复制字节（{@code getBytes()} 会先分配 byte[]）。 */
    private byte[] readBlob(FlexBuffers.Reference ref) {
        FlexBuffers.Blob blob = ref.asBlob();
        checkDeclaredLength(blob.size(), "Blob");
        return blob.getBytes();
    }

    private Collection<Object> readCollection(FlexBuffers.Reference ref, Type type, int depth) {
        Type elementType = extractElementType(type);
        FlexBuffers.Vector vec = ref.asVector();
        checkDeclaredLength(vec.size(), "collection element count");
        Collection<Object> result = newCollection(toClass(type));
        for (int i = 0; i < vec.size(); i++) {
            result.add(readValue(vec.get(i), elementType, depth + 1));
        }
        return result;
    }

    /** 目标类型为 Object 时，按 FlexBuffers 实际类型动态回塑（用于无泛型信息的集合/映射元素）。 */
    private Object readDynamic(FlexBuffers.Reference ref, int depth) {
        if (ref.isNull()) {
            return null;
        }
        if (ref.isVector()) {
            return readCollection(ref, List.class, depth);
        }
        if (ref.isMap()) {
            return readMap(ref, Map.class, depth);
        }
        if (ref.isString()) {
            return ref.asString();
        }
        if (ref.isBlob()) {
            return readBlob(ref);
        }
        if (ref.isBoolean()) {
            return ref.asBoolean();
        }
        if (ref.isInt() || ref.isUInt()) {
            return ref.asLong();
        }
        if (ref.isFloat()) {
            return ref.asFloat();
        }
        return null;
    }

    private Map<String, Object> readMap(FlexBuffers.Reference ref, Type type, int depth) {
        Type valueType = extractMapValueType(type);
        FlexBuffers.Map map = ref.asMap();
        checkDeclaredLength(map.size(), "map entry count");
        Map<String, Object> result = newMap(toClass(type));
        FlexBuffers.KeyVector keys = map.keys();
        checkDeclaredLength(keys.size(), "map key count");
        for (int i = 0; i < keys.size(); i++) {
            String k = keys.get(i).toString();
            result.put(k, readValue(map.get(k), valueType, depth + 1));
        }
        return result;
    }

    private Object readPojo(FlexBuffers.Reference ref, Class<?> clazz, int depth) {
        FlexBuffers.Map map = ref.asMap();
        Object instance = newInstance(clazz);
        Class<?> scan = clazz;
        while (scan != null && scan != Object.class) {
            for (Field field : scan.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                    continue;
                }
                FlexBuffers.Reference value = map.get(field.getName());
                if (value == null || value.isNull()) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    field.set(instance, readValue(value, field.getGenericType(), depth + 1));
                } catch (IllegalAccessException e) {
                    throw new SerializationException(
                            "FlatBuffers cannot set field "
                                    + field.getName()
                                    + ": "
                                    + e.getMessage(),
                            e);
                }
            }
            scan = scan.getSuperclass();
        }
        return instance;
    }

    /** 按声明类型构造集合：接口/抽象类按语义选择实现，具体类按其无参构造实例化。 */
    @SuppressWarnings("unchecked")
    private Collection<Object> newCollection(Class<?> type) {
        if (Objects.nonNull(type)
                && !type.isInterface()
                && !Modifier.isAbstract(type.getModifiers())) {
            try {
                return (Collection<Object>) type.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException | RuntimeException ex) {
                throw new SerializationException(
                        "FlatBuffers cannot instantiate collection type "
                                + type.getName()
                                + " (a no-arg constructor is required)",
                        ex);
            }
        }
        if (SortedSet.class.isAssignableFrom(type)) {
            return new TreeSet<>();
        }
        if (Set.class.isAssignableFrom(type)) {
            return new LinkedHashSet<>();
        }
        if (Deque.class.isAssignableFrom(type)) {
            return new ArrayDeque<>();
        }
        if (Queue.class.isAssignableFrom(type)) {
            return new ArrayDeque<>();
        }
        return new ArrayList<>();
    }

    /** 按声明类型构造映射：{@code SortedMap} → {@code TreeMap}，其余接口 → {@code LinkedHashMap}。 */
    @SuppressWarnings("unchecked")
    private Map<String, Object> newMap(Class<?> type) {
        if (Objects.nonNull(type)
                && !type.isInterface()
                && !Modifier.isAbstract(type.getModifiers())) {
            try {
                return (Map<String, Object>) type.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException | RuntimeException ex) {
                throw new SerializationException(
                        "FlatBuffers cannot instantiate map type "
                                + type.getName()
                                + " (a no-arg constructor is required)",
                        ex);
            }
        }
        if (SortedMap.class.isAssignableFrom(type)) {
            return new TreeMap<>();
        }
        return new LinkedHashMap<>();
    }

    private Object newInstance(Class<?> clazz) {
        if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            throw new SerializationException(
                    "FlatBuffers cannot instantiate abstract/interface type: " + clazz.getName());
        }
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new SerializationException(
                    "FlatBuffers requires a no-arg constructor for: " + clazz.getName(), e);
        }
    }

    // ============================================================
    // 不可信输入的边界防护
    // ============================================================

    /** 校验 FlexBuffers 载荷中声明的字段长度（长度字段可被写入方控制，必须先校验再分配）。 */
    private static void checkDeclaredLength(int declared, String what) {
        if (declared < 0 || declared > MAX_FIELD_LENGTH_BYTES) {
            throw new SerializationException(
                    "FlatBuffers rejected declared "
                            + what
                            + " = "
                            + declared
                            + ", which is outside [0, "
                            + MAX_FIELD_LENGTH_BYTES
                            + "] bytes (per-field limit)");
        }
    }

    /** 校验嵌套深度（读写对称）：超限直接拒绝，避免栈溢出 / 循环引用无限递归。 */
    private static void checkDepth(int depth, String phase) {
        if (depth > MAX_DEPTH) {
            throw new SerializationException(
                    "FlatBuffers "
                            + phase
                            + " rejected a structure nested deeper than "
                            + MAX_DEPTH
                            + " levels");
        }
    }

    // ============================================================
    // 类型解析工具
    // ============================================================

    private Class<?> toClass(Type type) {
        if (type instanceof Class) {
            return (Class<?>) type;
        } else if (type instanceof ParameterizedType) {
            return (Class<?>) ((ParameterizedType) type).getRawType();
        }
        return Object.class;
    }

    private Type extractElementType(Type collectionType) {
        if (collectionType instanceof ParameterizedType) {
            Type[] args = ((ParameterizedType) collectionType).getActualTypeArguments();
            if (args.length > 0) {
                return args[0];
            }
        }
        return Object.class;
    }

    private Type extractMapValueType(Type mapType) {
        if (mapType instanceof ParameterizedType) {
            Type[] args = ((ParameterizedType) mapType).getActualTypeArguments();
            if (args.length > 1) {
                return args[1];
            }
        }
        return Object.class;
    }
}
