/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import io.github.streammq.core.util.StringUtils;

/**
 * 载荷驱动类型解析的安全护栏（消费回退链 / 事务回查半消息共用）。
 *
 * <p><b>背景：</b>当消费者未声明目标类型（raw 实现无泛型实参）时，SDK 会从消息载荷的 {@code bodyType} / {@code bodyTypeName}
 * 字段解析反序列化目标类，以支持跨包/跨模块/跨语言 互操作。该字段位于 Redis 载荷中、可被写入方控制——若不做限制，攻击者可指定 classpath 上 的任意类作为反序列化目标（类型注入
 * / 反序列化攻击面）。
 *
 * <p><b>护栏：</b>拒绝 JDK 平台与主流框架的危险命名空间（这些命名空间承载已知 gadget 链， 且业务消息体几乎不可能声明为这些类型）。业务命名空间（{@code com.*}
 * / {@code io.*} / {@code org.mycompany.*} 等）不受影响，跨模块兼容特性保留。
 *
 * <p><b>推荐用法：</b>生产环境应为消费者声明显式目标类型（注解泛型或 {@code targetBodyType}），
 * 显式声明的类型<b>不经过</b>本护栏（来自代码而非载荷，可信）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public final class PayloadTypeSafety {

    /**
     * 载荷驱动的类型解析禁用前缀。
     *
     * <ul>
     *   <li>{@code java.*} / {@code javax.*} / {@code jdk.*}：JDK 平台类（Runtime、
     *       ProcessBuilder、JNDI、各类 readObject gadget 入口）
     *   <li>{@code sun.*} / {@code com.sun.*}：JDK 内部实现类（历史 gadget 链高发区）
     *   <li>{@code org.springframework.*}：Spring 框架类（Jackson/JDK 反序列化 gadget 链常见来源）
     * </ul>
     */
    private static final String[] BLOCKED_PREFIXES = {
        "java.", "javax.", "jdk.", "sun.", "com.sun.", "org.springframework."
    };

    private PayloadTypeSafety() {}

    /**
     * 判断载荷驱动的类型名是否被护栏拒绝。
     *
     * @param typeName 载荷中声明的类名（full name 或 simple name）
     * @return true 表示该类型名不得用于载荷驱动的反序列化目标解析
     */
    public static boolean isBlocked(String typeName) {
        if (StringUtils.isEmpty(typeName)) {
            return false;
        }
        for (String prefix : BLOCKED_PREFIXES) {
            if (typeName.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
