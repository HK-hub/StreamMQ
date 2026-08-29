/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.compression;

/**
 * 消息体压缩编解码器 SPI。
 *
 * <p>用于在消息发送时对 body 字节进行压缩，消费时解压，减少网络传输与 Redis 内存占用。 当 {@code ProducerConfig.compressThreshold} 大于
 * 0 且 body 字节数超过阈值时触发压缩。
 *
 * <p>内置实现：
 *
 * <ul>
 *   <li>{@code GzipCompressionCodec} - 基于 GZIP 的压缩（mandatory，{@code streammq-redisson} 模块默认注册）
 *   <li>{@code Lz4CompressionCodec} - 基于 LZ4 的压缩（条件性，<b>仅当 classpath 存在 {@code org.lz4:lz4-java}
 *       时自动注册</b>，无需用户写代码；否则 {@code streammq-redisson} 不引入任何 LZ4 编译期依赖，避免无 LZ4 需求用户被迫下载）
 * </ul>
 *
 * <p>用户启用 LZ4 仅需在业务工程中添加 {@code org.lz4:lz4-java} 依赖——{@code streammq-spring-boot-starter} 会通过
 * {@code Lz4CompressionCodecFactory.tryCreate()} 反射探测并自动注册 LZ4 Codec。
 *
 * <p>自定义实现：业务方可实现此接口注册为 Bean，框架自动注入到 Producer。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface CompressionCodec {

    /**
     * 压缩数据。
     *
     * @param data 原始字节数组
     * @return 压缩后的字节数组
     */
    byte[] compress(byte[] data);

    /**
     * 解压数据。
     *
     * @param data 压缩后的字节数组
     * @return 解压后的原始字节数组
     */
    byte[] decompress(byte[] data);

    /**
     * 返回编解码器名称，用于标识压缩算法。
     *
     * @return 名称（如 {@code gzip}；自定义实现可为 {@code lz4} / {@code zstd} / {@code snappy} 等）
     */
    String name();
}
