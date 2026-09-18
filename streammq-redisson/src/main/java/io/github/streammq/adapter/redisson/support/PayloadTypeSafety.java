/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import io.github.streammq.core.util.StringUtils;
import java.util.regex.Pattern;

/**
 * 载荷驱动类型解析的安全护栏（消费回退链 / 事务回查半消息共用）。
 *
 * <p><b>背景：</b>当消费者未声明目标类型（raw 实现无泛型实参）时，SDK 会从消息载荷的 {@code bodyType} / {@code bodyTypeName}
 * 字段解析反序列化目标类，以支持跨包/跨模块/跨语言 互操作。该字段位于 Redis 载荷中、可被写入方控制——若不做限制，攻击者可指定 classpath 上 的任意类作为反序列化目标（类型注入
 * / 反序列化攻击面）。
 *
 * <p><b>护栏策略（拒绝式前缀 + 形态校验）：</b>
 *
 * <ol>
 *   <li><b>形态归一化</b>：先剥离数组/描述符形态（{@code [Ljava.lang.Runtime;} → {@code java.lang.Runtime}）， 防止用 JVM
 *       描述符绕过前缀判断。
 *   <li><b>形态校验</b>：类型名必须是合法 Java 类名（拒绝 {@code $$Lambda}、动态代理 {@code $Proxy}、 以及任何非法标识符），非法形态一律拒绝。
 *   <li><b>危险命名空间</b>：拒绝 JDK 平台命名空间与已知 gadget 链命名空间（Commons-Collections / fastjson / Xalan /
 *       SnakeYAML / BeanUtils / Groovy / ROME 等）。
 * </ol>
 *
 * <p>业务命名空间（{@code com.*} / {@code io.*} / {@code org.mycompany.*} 等）不受影响，跨模块兼容特性保留。
 *
 * <p><b>定位（重要）：</b>本护栏是<b>命名空间拒绝式黑名单</b>，属于<b>纵深防御</b>，<b>不是完整性边界</b>—— 它只阻断已知危险命名空间，无法枚举所有
 * gadget；对<b>消费者显式声明的类型不做检查</b>（显式声明来自代码，可信）。 该护栏对<b>所有</b>序列化器的载荷驱动类型解析生效（消费者回退链 / 事务回查半消息共用，不仅
 * {@code JdkSerializer}）： 任何把载荷字段当作“反序列化目标类的来源”的实现都必须先经过 {@link #isBlocked(String)}。
 * 生产环境仍应始终为消费者显式声明类型 （注解泛型或 {@code targetBodyType}），把目标类固定在代码里、而非由载荷驱动。
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
     *   <li>{@code java.*} / {@code javax.*} / {@code jdk.*}：JDK 平台类（Runtime、ProcessBuilder、JNDI
     *       {@code javax.management}/LDAP、{@code java.lang.reflect}、{@code java.lang.invoke}、各类
     *       readObject gadget 入口）
     *   <li>{@code sun.*} / {@code com.sun.*}：JDK 内部实现类（历史 gadget 链高发区，含 JdbcRowSetImpl 所在的 {@code
     *       com.sun.rowset}）
     *   <li>{@code org.springframework.*}：Spring 框架类（Jackson/JDK 反序列化 gadget 链常见来源）
     *   <li>第三方知名 gadget 链命名空间：Commons-Collections / BeanUtils / Commons-IO / fastjson / Xalan /
     *       SnakeYAML / Groovy / ROME / XStream / Hibernate / Quartz / cglib / Javassist / Struts2
     *       / 脚本引擎等
     * </ul>
     *
     * <p>黑名单是纵深防御、不是完整性边界：未列出但存在 readObject/readResolve 钩子的业务类仍可能成为 gadget，因此正确的用法是消费者显式声明类型（见类
     * javadoc）。
     */
    private static final String[] BLOCKED_PREFIXES = {
        // JDK 平台（javax.* 已覆盖 javax.management/JNDI，com.sun.* 已覆盖 com.sun.rowset/JdbcRowSetImpl）
        "java.",
        "javax.",
        "jdk.",
        "sun.",
        "com.sun.",
        // 主流框架
        "org.springframework.",
        // 已知 gadget 链命名空间
        "org.apache.commons.collections.",
        "org.apache.commons.collections4.",
        "org.apache.commons.beanutils.",
        "org.apache.commons.configuration.",
        "org.apache.commons.io.",
        "org.apache.commons.jelly.",
        "org.apache.commons.fileupload.",
        "org.apache.commons.dbcp.",
        "org.apache.commons.dbcp2.",
        "org.apache.commons.text.",
        "org.apache.xalan.",
        "org.apache.xpath.",
        "org.apache.velocity.",
        "org.apache.ignite.",
        "org.apache.activemq.",
        "org.apache.myfaces.",
        "org.apache.struts.",
        "org.apache.tomcat.",
        "org.apache.log4j.",
        "org.yaml.snakeyaml.",
        "com.alibaba.fastjson.",
        "com.alibaba.fastjson2.",
        "com.thoughtworks.xstream.",
        "com.rometools.rome.",
        "com.google.common.collect.",
        "com.mchange.v2.",
        "com.zaxxer.hikari.",
        "net.sf.json.",
        "net.sf.cglib.",
        "net.sf.ehcache.",
        "org.json.",
        "org.hibernate.",
        "org.quartz.",
        "org.jboss.",
        "org.javassist.",
        "org.codehaus.groovy.",
        "groovy.util.",
        "org.mozilla.javascript.",
        "org.python.",
        "ch.qos.logback."
    };

    /** 合法 Java 类名（含内部类 {@code $}、包名点分） */
    private static final Pattern VALID_CLASS_NAME =
            Pattern.compile("^[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)*$");

    private PayloadTypeSafety() {}

    /**
     * 归一化类型名：剥离 JVM 数组/对象描述符形态，得到裸类名。
     *
     * <p>{@code [Ljava.lang.Runtime;} / {@code [[Lcom.foo.Bar;} / {@code Ljava.lang.String;} /
     * {@code java.lang.String;} 均归一到不带前缀与分号的类名。
     *
     * @param typeName 原始类型名
     * @return 归一化后的类名（入参为空时原样返回）
     */
    public static String normalize(String typeName) {
        if (StringUtils.isEmpty(typeName)) {
            return typeName;
        }
        String name = typeName.trim();
        while (name.startsWith("[")) {
            name = name.substring(1);
        }
        if (name.startsWith("L") && name.endsWith(";")) {
            name = name.substring(1, name.length() - 1);
        }
        if (name.endsWith(";")) {
            name = name.substring(0, name.length() - 1);
        }
        return name;
    }

    /**
     * 判断载荷驱动的类型名是否被护栏拒绝。
     *
     * @param typeName 载荷中声明的类名（full name / simple name / JVM 描述符形态）
     * @return true 表示该类型名不得用于载荷驱动的反序列化目标解析
     */
    public static boolean isBlocked(String typeName) {
        if (StringUtils.isEmpty(typeName)) {
            return false;
        }
        String name = normalize(typeName);
        if (StringUtils.isEmpty(name)) {
            return true;
        }
        // 形态校验：必须为合法类名；并额外拒绝 lambda / 动态代理形态
        if (!VALID_CLASS_NAME.matcher(name).matches()) {
            return true;
        }
        int lastDot = name.lastIndexOf('.');
        String simpleName = lastDot >= 0 ? name.substring(lastDot + 1) : name;
        if (simpleName.contains("$$Lambda") || simpleName.startsWith("$Proxy")) {
            return true;
        }
        for (String prefix : BLOCKED_PREFIXES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
