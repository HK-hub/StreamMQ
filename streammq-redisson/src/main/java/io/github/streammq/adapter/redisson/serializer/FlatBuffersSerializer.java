/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import com.google.flatbuffers.FlexBuffers;
import com.google.flatbuffers.FlexBuffersBuilder;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
 *   <li>不支持的类型（如无法实例化的抽象类、无无参构造的 POJO）会抛出 {@link SerializationException}。
 * </ul>
 *
 * <p><b>⚠️ 安全：</b>FlexBuffers 仅物化声明数据，反序列化不会实例化 classpath 上任意类（与 Jackson 严格类型、 Protostuff 同源），无
 * gadget RCE 面。需选用本序列化器时，用户需在自身依赖中加入 {@code com.google.flatbuffers:flatbuffers-java}（本模块以 optional
 * 声明，不强制传递）。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FlatBuffersSerializer<T> implements MessageSerializer<T> {

    /** 顶层值统一放在根 Map 的该键下。 */
    private static final String ROOT_KEY = "v";

    @Override
    public byte[] serialize(T object, Class<T> type) throws SerializationException {
        if (object == null) {
            return null;
        }
        try {
            FlexBuffersBuilder builder = new FlexBuffersBuilder();
            int root = builder.startMap();
            writeValue(builder, ROOT_KEY, object, type);
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
        if (bytes == null) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            FlexBuffers.Reference root = FlexBuffers.getRoot(buffer);
            if (root.isNull()) {
                return null;
            }
            FlexBuffers.Reference value = root.asMap().get(ROOT_KEY);
            return readValue(value, type);
        } catch (SerializationException ex) {
            throw ex;
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

    private void writeValue(FlexBuffersBuilder b, String key, Object value, Type genericType) {
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
            writeCollection(b, key, (Collection<?>) value, genericType);
        } else if (value instanceof Map) {
            writeMap(b, key, (Map<?, ?>) value, genericType);
        } else if (value.getClass().isArray()) {
            writeArray(b, key, value);
        } else {
            // 任意 POJO：递归写入其字段
            int m = b.startMap();
            writeFields(b, value);
            b.endMap(key, m);
        }
    }

    /** 写入集合元素（无 key，attach 到当前打开的 Vector）。 */
    private void writeElement(FlexBuffersBuilder b, Object value, Type elementType) {
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
            writeCollection(b, null, (Collection<?>) value, elementType);
        } else if (value instanceof Map) {
            writeMap(b, null, (Map<?, ?>) value, elementType);
        } else if (value.getClass().isArray()) {
            writeArray(b, null, value);
        } else {
            int m = b.startMap();
            writeFields(b, value);
            b.endMap(null, m);
        }
    }

    private void writeCollection(
            FlexBuffersBuilder b, String key, Collection<?> value, Type genericType) {
        Type elementType = extractElementType(genericType);
        int v = b.startVector();
        for (Object e : value) {
            writeElement(b, e, elementType);
        }
        b.endVector(key, v, false, false);
    }

    private void writeMap(FlexBuffersBuilder b, String key, Map<?, ?> value, Type genericType) {
        Type valueType = extractMapValueType(genericType);
        int m = b.startMap();
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            String k = String.valueOf(entry.getKey());
            writeValue(b, k, entry.getValue(), valueType);
        }
        b.endMap(key, m);
    }

    private void writeArray(FlexBuffersBuilder b, String key, Object array) {
        int len = Array.getLength(array);
        int v = b.startVector();
        for (int i = 0; i < len; i++) {
            writeElement(b, Array.get(array, i), null);
        }
        b.endVector(key, v, false, false);
    }

    private void writeFields(FlexBuffersBuilder b, Object object) {
        Class<?> clazz = object.getClass();
        while (clazz != null && clazz != Object.class) {
            for (Field field : clazz.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
                        || field.isSynthetic()) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    writeValue(b, field.getName(), field.get(object), field.getGenericType());
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
    private <R> R readValue(FlexBuffers.Reference ref, Type type) {
        if (ref == null || ref.isNull()) {
            return null;
        }
        Class<?> clazz = toClass(type);
        if (clazz == Object.class) {
            return (R) readDynamic(ref);
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
            return (R) ref.asBlob().getBytes();
        } else if (Collection.class.isAssignableFrom(clazz)) {
            return (R) readCollection(ref, type);
        } else if (Map.class.isAssignableFrom(clazz)) {
            return (R) readMap(ref, type);
        } else {
            return (R) readPojo(ref, clazz);
        }
    }

    private Collection<Object> readCollection(FlexBuffers.Reference ref, Type type) {
        Type elementType = extractElementType(type);
        FlexBuffers.Vector vec = ref.asVector();
        List<Object> list = new ArrayList<>(vec.size());
        for (int i = 0; i < vec.size(); i++) {
            list.add(readValue(vec.get(i), elementType));
        }
        return list;
    }

    /** 目标类型为 Object 时，按 FlexBuffers 实际类型动态回塑（用于无泛型信息的集合/映射元素）。 */
    private Object readDynamic(FlexBuffers.Reference ref) {
        if (ref.isNull()) {
            return null;
        }
        if (ref.isVector()) {
            return readCollection(ref, Object.class);
        }
        if (ref.isMap()) {
            return readMap(ref, Object.class);
        }
        if (ref.isString()) {
            return ref.asString();
        }
        if (ref.isBlob()) {
            return ref.asBlob().getBytes();
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

    private Map<String, Object> readMap(FlexBuffers.Reference ref, Type type) {
        Type valueType = extractMapValueType(type);
        FlexBuffers.Map map = ref.asMap();
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        FlexBuffers.KeyVector keys = map.keys();
        for (int i = 0; i < keys.size(); i++) {
            String k = keys.get(i).toString();
            result.put(k, readValue(map.get(k), valueType));
        }
        return result;
    }

    private Object readPojo(FlexBuffers.Reference ref, Class<?> clazz) {
        FlexBuffers.Map map = ref.asMap();
        Object instance = newInstance(clazz);
        Class<?> scan = clazz;
        while (scan != null && scan != Object.class) {
            for (Field field : scan.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
                        || field.isSynthetic()) {
                    continue;
                }
                FlexBuffers.Reference value = map.get(field.getName());
                if (value == null || value.isNull()) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    field.set(instance, readValue(value, field.getGenericType()));
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

    private Object newInstance(Class<?> clazz) {
        if (clazz.isInterface() || java.lang.reflect.Modifier.isAbstract(clazz.getModifiers())) {
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
