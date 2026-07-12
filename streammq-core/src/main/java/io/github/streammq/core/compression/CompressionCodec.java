package io.github.streammq.core.compression;

/**
 * 消息体压缩编解码器 SPI。
 *
 * <p>用于在消息发送时对 body 字节进行压缩，消费时解压，减少网络传输与 Redis 内存占用。
 * 当 {@code ProducerConfig.compressThreshold} 大于 0 且 body 字节数超过阈值时触发压缩。
 *
 * <p>内置实现：
 * <ul>
 *   <li>{@code GzipCompressionCodec} - 基于 GZIP 的压缩（mandatory）</li>
 *   <li>{@code Lz4CompressionCodec} - 基于 LZ4 的压缩（optional，需引入 LZ4 依赖）</li>
 * </ul>
 *
 * <p>自定义实现：业务方可实现此接口注册为 Bean，框架自动注入到 Producer。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
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
     * @return 名称（如 {@code gzip}、{@code lz4}）
     */
    String name();
}
