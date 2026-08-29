/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.compression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Lz4CompressionCodec} 工厂：按 classpath 探测结果条件性创建 Codec 实例。
 *
 * <p>本类提供两个静态入口供 {@code streammq-spring-boot-starter} 等装配层使用：
 *
 * <ul>
 *   <li>{@link #isAvailable()} — classpath 是否存在 lz4-java（不触发 LZ4Factory 静态初始化）
 *   <li>{@link #tryCreate()} — 若可用则返回 Codec 实例，否则返回 {@code null}（不抛异常）
 * </ul>
 *
 * <p>使用方式：
 *
 * <pre>{@code
 * Lz4CompressionCodec lz4 = Lz4CompressionCodecFactory.tryCreate();
 * if (lz4 != null) {
 *     registry.register(lz4);
 * }
 * }</pre>
 *
 * <p>本类不持有任何 lz4-java 编译期依赖，所有探测均通过 {@link Class#forName(String, boolean, ClassLoader)} 完成。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class Lz4CompressionCodecFactory {

    private static final Logger LOG = LoggerFactory.getLogger(Lz4CompressionCodecFactory.class);

    /** LZ4 工厂类全限定名（运行时按需加载），与 {@link Lz4CompressionCodec#LZ4_FACTORY_CLASS} 保持一致。 */
    private static final String LZ4_FACTORY_CLASS = Lz4CompressionCodec.LZ4_FACTORY_CLASS;

    private Lz4CompressionCodecFactory() {
        // utility class
    }

    /**
     * classpath 是否存在 lz4-java。
     *
     * <p>该方法仅做 {@code Class.forName(name, false, classLoader)} 探测——<b>不会触发</b> {@code LZ4Factory}
     * 的静态初始化，避免冷启动时拉起 LZ4 native 资源。返回 {@code true} 不保证构造 {@link Lz4CompressionCodec}
     * 一定成功（极端情况下反射方法签名不匹配），但生产中可视为等价。
     *
     * @return true 表示 lz4-java 在 classpath
     */
    public static boolean isAvailable() {
        return isClassPresent(LZ4_FACTORY_CLASS);
    }

    /**
     * 尝试创建一个 {@link Lz4CompressionCodec}。
     *
     * <p>若 lz4-java 不在 classpath 返回 {@code null}（<b>不抛异常</b>），便于装配层无侵入地条件性注册。 若 lz4-java
     * 存在但反射初始化失败（极少见，多为版本不兼容），记录 WARN 日志并返回 {@code null}。
     *
     * @return Codec 实例，若 LZ4 不可用或初始化失败则返回 {@code null}
     */
    public static Lz4CompressionCodec tryCreate() {
        if (!isAvailable()) {
            return null;
        }
        try {
            return new Lz4CompressionCodec();
        } catch (IllegalStateException ex) {
            // 反射初始化失败（极少：lz4-java 版本过旧/过新导致方法签名不匹配）。
            // 装配层应当作为可选 Codec 优雅降级，而非阻断启动。
            LOG.warn(
                    "LZ4 compression codec detected on classpath but failed to initialize: {}",
                    ex.getMessage());
            return null;
        }
    }

    /**
     * 探测 classpath 是否存在指定类，<b>不触发</b>该类的静态初始化。
     *
     * <p>优先使用线程上下文 ClassLoader（容器/应用服务器场景），回退到当前类 ClassLoader。
     */
    private static boolean isClassPresent(String className) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = Lz4CompressionCodecFactory.class.getClassLoader();
        }
        try {
            Class.forName(className, false, cl);
            return true;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }
}
