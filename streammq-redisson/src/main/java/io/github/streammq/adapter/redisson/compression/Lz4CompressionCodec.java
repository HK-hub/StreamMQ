/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.compression;

import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.exception.StreamMQException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * 基于 LZ4 的 {@link CompressionCodec} 实现（条件性注册）。
 *
 * <p>该 Codec 由 {@link Lz4CompressionCodecFactory#tryCreate()} 在以下条件成立时自动注册到 {@link
 * DefaultCompressionCodecRegistry}：
 *
 * <ul>
 *   <li>classpath 上存在 {@code net.jpountz.lz4.LZ4Factory}（即用户引入了 {@code org.lz4:lz4-java} 依赖）
 * </ul>
 *
 * <p>本类<b>不持有对 lz4-java 任何类型的编译期引用</b>——所有 LZ4 类与方法均通过 {@link Class#forName(String)} + {@link
 * Method#invoke(Object, Object...)} 反射加载。这避免将 LZ4 强加给所有 streammq-redisson 用户。
 *
 * <h3>线协议</h3>
 *
 * <p>为了在不依赖 {@code LZ4SafeDecompressor} 长度前缀的前提下支持 {@code LZ4FastDecompressor}，本 Codec 在 压缩前于结果前缀写入
 * 4 字节大端 {@code int}（原始未压缩长度），消费者先读取该长度再分配缓冲调用 fast decompress：
 *
 * <pre>
 *   [0..4)   : 4-byte big-endian original length (int)
 *   [4..N)   : raw LZ4 fastCompressor output
 * </pre>
 *
 * <p>此格式与 {@code LZ4SafeDecompressor} 不互通；如需兼容，请改用 {@code Lz4SafeCompressionCodec}（社区实现）。
 *
 * <h3>解压炸弹防护</h3>
 *
 * <p>解压时强制校验头部的原始长度不超过 {@link #MAX_EXPANDED_BYTES}（64MB），与 {@link GzipCompressionCodec} 同样的防
 * zip-bomb 策略。
 *
 * <p><b>异常约定：</b>{@link IllegalStateException} 仅在构造时（LZ4 不在 classpath）抛出；运行时 压缩/解压失败抛出 {@link
 * StreamMQException} 或 {@link SerializationException}，由消费侧路由到 DLQ。
 *
 * <p>线程安全：缓存的 {@code Method} 句柄与底层 LZ4 compressor/decompressor 实例无状态，可在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class Lz4CompressionCodec implements CompressionCodec {

    /** Codec 协议标识（写入 {@code compressed} 字段的值）。 */
    public static final String CODEC_NAME = "lz4";

    /**
     * 解压输出上限（64MB）：防御解压炸弹（zip bomb）。
     *
     * <p>LZ4 fast 路径无内置长度上限校验，故需在应用层强制。恶意/损坏的 LZ4 流若头部长度被篡改为巨大值，可能导致 OOM。 头部声明的原始长度超过该上限立即抛 {@link
     * SerializationException}——上游毒丸消息路径会将其路由到 DLQ，不会拖垮消费线程。
     */
    public static final long MAX_EXPANDED_BYTES = 64L * 1024 * 1024;

    /** LZ4 工厂类全限定名（运行时按需加载）。 */
    static final String LZ4_FACTORY_CLASS = "net.jpountz.lz4.LZ4Factory";

    /** 缓存的 compressor 实例（{@code LZ4Compressor}），构造时反射初始化。 */
    private final Object compressor;

    /** 缓存的 decompressor 实例（{@code LZ4FastDecompressor}），构造时反射初始化。 */
    private final Object decompressor;

    /** {@code LZ4Compressor.maxCompressedLength(int)} 反射方法句柄。 */
    private final Method maxCompressedLengthMethod;

    /** {@code LZ4Compressor.compress(byte[],int,int,byte[],int,int)} 反射方法句柄。 */
    private final Method compressMethod;

    /** {@code LZ4FastDecompressor.decompress(byte[],int,byte[],int,int)} 反射方法句柄。 */
    private final Method decompressMethod;

    /**
     * 构造 Codec：通过 {@code Class.forName} 检测 lz4-java 是否在 classpath，并反射初始化 compressor/decompressor。
     *
     * <p>若 LZ4 不在 classpath，立即抛 {@link IllegalStateException}——这正是「条件性」注册的关键判断。 业务上无需直接 new 该类：使用
     * {@link Lz4CompressionCodecFactory#tryCreate()} 即可在 LZ4 不可用时返回 {@code null} 而不抛异常。
     *
     * @throws IllegalStateException 若 lz4-java 不在 classpath，或反射初始化失败
     */
    public Lz4CompressionCodec() {
        try {
            Class<?> factoryClass = Class.forName(LZ4_FACTORY_CLASS);
            // LZ4Factory.fastestInstance() -> LZ4Factory
            Object factory = factoryClass.getMethod("fastestInstance").invoke(null);
            // LZ4Factory.fastCompressor() -> LZ4Compressor
            this.compressor = factoryClass.getMethod("fastCompressor").invoke(factory);
            // LZ4Factory.fastDecompressor() -> LZ4FastDecompressor
            this.decompressor = factoryClass.getMethod("fastDecompressor").invoke(factory);

            Class<?> compressorClass = Class.forName("net.jpountz.lz4.LZ4Compressor");
            this.maxCompressedLengthMethod =
                    compressorClass.getMethod("maxCompressedLength", int.class);
            this.compressMethod =
                    compressorClass.getMethod(
                            "compress",
                            byte[].class,
                            int.class,
                            int.class,
                            byte[].class,
                            int.class,
                            int.class);

            Class<?> decompressorClass = Class.forName("net.jpountz.lz4.LZ4FastDecompressor");
            this.decompressMethod =
                    decompressorClass.getMethod(
                            "decompress",
                            byte[].class,
                            int.class,
                            byte[].class,
                            int.class,
                            int.class);
        } catch (ClassNotFoundException ex) {
            // 关键：清晰提示用户需添加依赖。LZF4CompressionCodecFactory.tryCreate 会捕获该异常并返回 null。
            throw new IllegalStateException(
                    "LZ4 library not found on classpath. Add org.lz4:lz4-java dependency to use"
                            + " LZ4 compression",
                    ex);
        } catch (NoSuchMethodException | IllegalAccessException ex) {
            throw new IllegalStateException(
                    "Failed to reflectively initialize LZ4 codec: incompatible lz4-java version",
                    ex);
        } catch (InvocationTargetException ex) {
            throw new IllegalStateException(
                    "Failed to reflectively initialize LZ4 codec: " + ex.getCause(), ex.getCause());
        }
    }

    @Override
    public byte[] compress(byte[] data) {
        Objects.requireNonNull(data, "data");
        if (data.length == 0) {
            return data;
        }
        try {
            int maxCompressedLength =
                    (int) maxCompressedLengthMethod.invoke(compressor, data.length);
            // 头部 4 字节存原始长度 + LZ4 压缩数据
            byte[] out = new byte[Integer.BYTES + maxCompressedLength];
            ByteBuffer.wrap(out, 0, Integer.BYTES).putInt(data.length);
            int written =
                    (int)
                            compressMethod.invoke(
                                    compressor,
                                    data,
                                    0,
                                    data.length,
                                    out,
                                    Integer.BYTES,
                                    maxCompressedLength);
            byte[] result = new byte[Integer.BYTES + written];
            System.arraycopy(out, 0, result, 0, result.length);
            return result;
        } catch (InvocationTargetException ex) {
            throw new StreamMQException("LZ4 compress failed", ex.getCause());
        } catch (IllegalAccessException ex) {
            throw new StreamMQException("LZ4 compress failed", ex);
        }
    }

    @Override
    public byte[] decompress(byte[] data) {
        Objects.requireNonNull(data, "data");
        if (data.length == 0) {
            return data;
        }
        if (data.length < Integer.BYTES) {
            throw new SerializationException(
                    "LZ4 payload too short (missing 4-byte length header): " + data.length);
        }
        int originalLength = ByteBuffer.wrap(data, 0, Integer.BYTES).getInt();
        if (originalLength < 0 || originalLength > MAX_EXPANDED_BYTES) {
            // zip-bomb 防护：拒绝声明了过大原始长度的毒丸消息
            throw new SerializationException(
                    "LZ4 decompressed size "
                            + originalLength
                            + " bytes exceeds limit "
                            + MAX_EXPANDED_BYTES
                            + " bytes");
        }
        try {
            byte[] result = new byte[originalLength];
            decompressMethod.invoke(decompressor, data, Integer.BYTES, result, 0, originalLength);
            return result;
        } catch (InvocationTargetException ex) {
            // 常见原因：corrupt LZ4 stream / declared length mismatch
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException) {
                throw new SerializationException(
                        "LZ4 decompress failed: " + cause.getMessage(), cause);
            }
            throw new StreamMQException("LZ4 decompress failed", cause);
        } catch (IllegalAccessException ex) {
            throw new StreamMQException("LZ4 decompress failed", ex);
        }
    }

    @Override
    public String name() {
        return CODEC_NAME;
    }
}
