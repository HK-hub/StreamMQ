/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.annotation.AnnotationAttributeResolver;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.annotation.StreamMQTransactionConsumer;
import io.github.streammq.core.consumer.DlqMessageConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.transaction.TransactionChecker;
import io.github.streammq.spring.boot.support.SpringAnnotationAttributeResolver;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;

/**
 * 在所有单例 Bean 初始化完成后，扫描带有 {@link StreamMQConsumer} / {@link StreamMQTransactionConsumer} 注解的
 * Bean，将它们注册到 {@link DefaultStreamMQListenerContainer} 与 {@link TransactionScanner}。
 *
 * <p>注册顺序：
 *
 * <ol>
 *   <li>扫描 {@code @StreamMQConsumer} 标注的 Bean：
 *       <ul>
 *         <li>若 {@code messageModel = ORDERLY} 且实现 {@link StreamMessageOrderlyConsumer}，注册为顺序消费者
 *         <li>否则若实现 {@link StreamMessageConcurrentlyConsumer}，注册为并发消费者（含 DLQ 场景，由 {@code dlqMode}
 *             是否为 true 区分）
 *       </ul>
 *   <li>扫描 {@code @StreamMQTransactionConsumer} 标注的 Bean（实现 {@link TransactionChecker}）， 注册到 {@link
 *       TransactionScanner}
 *   <li>将容器内所有 Listener 的 (topic, group, maxReconsumeTimes) 注册到 RetryScheduler（若存在）
 * </ol>
 *
 * <p>容器的实际启动由 {@code SmartLifecycle} 完成，本类仅负责注册。
 *
 * <p>支持 ${...} 属性占位符与 #{...} SpEL 表达式：在注册前对注解中的
 * topic、consumerGroup、namespace、selectorExpression、transactionGroup 等字符串属性进行解析。
 *
 * <p>发现机制对 JDK/CGLIB 代理安全：通过 {@link AopUtils#getTargetClass} / {@link ClassUtils#getUserClass}
 * 解析目标类，并以 {@link AnnotatedElementUtils#findMergedAnnotation} 读取合并注解。任何 (topic, group)
 * 重复注册在注册开始前快速失败，避免静默覆盖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQListenerRegistrar
        implements SmartInitializingSingleton, ApplicationContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMQListenerRegistrar.class);

    private final DefaultStreamMQListenerContainer listenerContainer;
    private ApplicationContext applicationContext;

    /** 注解属性解析器（支持 ${} 占位符与 #{} SpEL 表达式） */
    private AnnotationAttributeResolver attributeResolver;

    /**
     * 构造 Registrar。
     *
     * @param listenerContainer Listener 容器
     */
    public StreamMQListenerRegistrar(DefaultStreamMQListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        // 初始化注解属性解析器（支持 ${} 占位符与 #{} SpEL 表达式）
        if (applicationContext instanceof ConfigurableApplicationContext cac) {
            this.attributeResolver = new SpringAnnotationAttributeResolver(cac);
        }
    }

    /**
     * 解析字符串属性值（支持 ${} 占位符与 #{} SpEL 表达式）。 若 attributeResolver 未初始化，则返回原值。
     *
     * @param value 原始值
     * @return 解析后的值
     */
    private String resolveAttribute(String value) {
        if (attributeResolver == null) {
            return value;
        }
        return attributeResolver.resolve(value);
    }

    /**
     * 创建 {@link StreamMQConsumer} 注解的动态代理，覆盖所有字符串类型属性为解析后的值， 其余方法委托给原注解。
     *
     * <p>支持 ${} 占位符与 #{} SpEL 表达式的属性：topic、consumerGroup、namespace、
     * selectorExpression、consumerName。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMQConsumer resolveStreamMQListener(StreamMQConsumer original) {
        String resolvedTopic = resolveAttribute(original.topic());
        String resolvedGroup = resolveAttribute(original.consumerGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        String resolvedSelector = resolveAttribute(original.selectorExpression());
        String resolvedConsumerName = resolveAttribute(original.consumerName());
        // 若无需解析，直接返回原注解
        if (resolvedTopic.equals(original.topic())
                && resolvedGroup.equals(original.consumerGroup())
                && resolvedNamespace.equals(original.namespace())
                && resolvedSelector.equals(original.selectorExpression())
                && resolvedConsumerName.equals(original.consumerName())) {
            return original;
        }
        LOG.info(
                "Resolved @StreamMQConsumer attributes: topic={} -> {}, consumerGroup={} -> {},"
                        + " namespace={} -> {}, selectorExpression={} -> {}, consumerName={} -> {}",
                original.topic(),
                resolvedTopic,
                original.consumerGroup(),
                resolvedGroup,
                original.namespace(),
                resolvedNamespace,
                original.selectorExpression(),
                resolvedSelector,
                original.consumerName(),
                resolvedConsumerName);
        final String fTopic = resolvedTopic;
        final String fGroup = resolvedGroup;
        final String fNamespace = resolvedNamespace;
        final String fSelector = resolvedSelector;
        final String fConsumerName = resolvedConsumerName;
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "topic":
                            return fTopic;
                        case "consumerGroup":
                            return fGroup;
                        case "namespace":
                            return fNamespace;
                        case "selectorExpression":
                            return fSelector;
                        case "consumerName":
                            return fConsumerName;
                        default:
                            return method.invoke(original, args);
                    }
                };
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        handler);
    }

    /**
     * 创建 {@link StreamMQTransactionConsumer} 注解的动态代理，覆盖 transactionGroup/namespace
     * 为解析后的值，其余方法委托给原注解。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMQTransactionConsumer resolveStreamMQTransactionListener(
            StreamMQTransactionConsumer original) {
        String resolvedTxGroup = resolveAttribute(original.transactionGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        if (resolvedTxGroup.equals(original.transactionGroup())
                && resolvedNamespace.equals(original.namespace())) {
            return original;
        }
        LOG.info(
                "Resolved @StreamMQTransactionConsumer attributes: transactionGroup={} -> {},"
                        + " namespace={} -> {}",
                original.transactionGroup(),
                resolvedTxGroup,
                original.namespace(),
                resolvedNamespace);
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "transactionGroup":
                            return resolvedTxGroup;
                        case "namespace":
                            return resolvedNamespace;
                        default:
                            return method.invoke(original, args);
                    }
                };
        return (StreamMQTransactionConsumer)
                Proxy.newProxyInstance(
                        StreamMQTransactionConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQTransactionConsumer.class},
                        handler);
    }

    @Override
    public void afterSingletonsInstantiated() {
        registerStreamMQListeners();
        registerDlqListeners();
        registerTransactionListeners();
        registerRetryTargetsIfPossible();
        LOG.info(
                "StreamMQ listener registration completed, total registrations={}",
                listenerContainer.getConsumers().size());
    }

    /** 若 RetryScheduler 已注册，将容器内所有 (topic, group, maxReconsumeTimes) 注册到 RetryScheduler。 */
    private void registerRetryTargetsIfPossible() {
        try {
            io.github.streammq.adapter.redisson.scheduler.RetryScheduler retryScheduler =
                    applicationContext.getBean(
                            io.github.streammq.adapter.redisson.scheduler.RetryScheduler.class);
            listenerContainer.registerRetryTargets(retryScheduler);
            LOG.info("Registered retry targets to RetryScheduler");
        } catch (org.springframework.beans.factory.NoSuchBeanDefinitionException ex) {
            LOG.debug("RetryScheduler not present, skip retry target registration");
        }
    }

    /**
     * 扫描 {@code @StreamMQConsumer} 标注的 Bean，按 {@code messageModel} 与实现的接口区分并发 / 顺序模式， 按 {@code
     * dlqMode} 是否为 true 区分 DLQ 消费者。
     *
     * <p>注册分两个阶段：
     *
     * <ol>
     *   <li>预检阶段：代理安全地解析全部候选 Bean 的注解（含 ${} / #{} 占位符）， 收集所有解析后的 (topic, group) 组合；发现重复组合立即抛出
     *       {@link IllegalStateException}，同时点名冲突的两个 Bean 类， 避免容器内静默覆盖注册项。
     *   <li>注册阶段：校验接口契约后逐一注册。注解存在但未实现任何 Consumer 接口的 Bean 视为配置错误， 快速失败而非静默忽略。
     * </ol>
     *
     * <p>{@code @SuppressWarnings("unchecked")} 原因：Spring ApplicationContext 返回的 Bean 类型为 Object，
     * 需要强制转换为具体的 Consumer 接口类型。由于 Java 泛型擦除，编译器无法在运行时验证转换安全性， 但此处已通过 {@code instanceof} 检查确保类型安全。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void registerStreamMQListeners() {
        Map<String, Object> beans =
                applicationContext.getBeansWithAnnotation(StreamMQConsumer.class);
        Map<String, Candidate> candidates = new LinkedHashMap<>();
        // 先收集 DLQ 消费者占用的注册键（容器内 DLQ 以 topic=group 写入同一注册表），参与统一预检
        collectDlqOccupiedKeys(candidates);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            Class<?> targetClass = resolveTargetClass(bean);
            StreamMQConsumer annotation =
                    AnnotatedElementUtils.findMergedAnnotation(targetClass, StreamMQConsumer.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式，得到最终的 topic/consumerGroup/namespace/selectorExpression
            StreamMQConsumer resolved = resolveStreamMQListener(annotation);
            if (!resolved.enable()) {
                LOG.info(
                        "Skip disabled @StreamMQConsumer: bean={}, topic={}",
                        beanName,
                        resolved.topic());
                continue;
            }
            checkDuplicateRegistration(
                    candidates,
                    pairKey(resolved.topic(), resolved.consumerGroup()),
                    beanName,
                    targetClass);
            candidates.put(
                    pairKey(resolved.topic(), resolved.consumerGroup()),
                    new Candidate(beanName, bean, targetClass, resolved));
        }
        for (Candidate candidate : candidates.values()) {
            StreamMQConsumer resolved = (StreamMQConsumer) candidate.annotation();
            boolean isOrderly = resolved.messageModel() == MessageModel.ORDERLY;
            if (isOrderly && candidate.bean() instanceof StreamMessageOrderlyConsumer listener) {
                listenerContainer.registerOrderlyConsumer(listener, resolved);
                LOG.info(
                        "Registered OrderlyConsumer: bean={}, topic={}, group={}",
                        candidate.beanName(),
                        resolved.topic(),
                        resolved.consumerGroup());
            } else if (candidate.bean() instanceof StreamMessageConcurrentlyConsumer listener) {
                listenerContainer.registerConsumer(listener, resolved);
                LOG.info(
                        "Registered Consumer: bean={}, topic={}, group={}",
                        candidate.beanName(),
                        resolved.topic(),
                        resolved.consumerGroup());
            } else {
                throw new IllegalStateException(
                        "Bean "
                                + candidate.beanName()
                                + " ("
                                + candidate.targetClass().getName()
                                + ") annotated with @StreamMQConsumer must implement"
                                + " StreamMessageConcurrentlyConsumer or"
                                + " StreamMessageOrderlyConsumer");
            }
        }
    }

    /** 扫描 {@code @StreamMQDlqConsumer} 标注的 Bean，校验实现 {@link DlqMessageConsumer}，注册到容器。 */
    private void registerDlqListeners() {
        Map<String, Object> beans =
                applicationContext.getBeansWithAnnotation(StreamMQDlqConsumer.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            Class<?> targetClass = resolveTargetClass(bean);
            StreamMQDlqConsumer annotation =
                    AnnotatedElementUtils.findMergedAnnotation(
                            targetClass, StreamMQDlqConsumer.class);
            if (annotation == null) {
                continue;
            }
            // 与消费者路径保持一致：解析 ${} 占位符与 #{} SpEL 表达式
            StreamMQDlqConsumer resolved = resolveStreamMQDlqListener(annotation);
            if (!resolved.enable()) {
                LOG.info(
                        "Skip disabled @StreamMQDlqConsumer: bean={}, group={}",
                        beanName,
                        resolved.consumerGroup());
                continue;
            }
            if (!(bean instanceof DlqMessageConsumer)) {
                throw new IllegalStateException(
                        "Bean "
                                + beanName
                                + " annotated with @StreamMQDlqConsumer must implement"
                                + " DlqMessageConsumer");
            }
            DlqMessageConsumer<?> listener = (DlqMessageConsumer<?>) bean;
            listenerContainer.registerDlqConsumer(listener, resolved);
            LOG.info(
                    "Registered DlqConsumer: bean={}, group={}",
                    beanName,
                    resolved.consumerGroup());
        }
    }

    /**
     * 创建 {@link StreamMQDlqConsumer} 注解的动态代理，覆盖 consumerGroup/namespace 为占位符解析后的值， 其余方法委托给原注解。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMQDlqConsumer resolveStreamMQDlqListener(StreamMQDlqConsumer original) {
        String resolvedGroup = resolveAttribute(original.consumerGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        if (resolvedGroup.equals(original.consumerGroup())
                && resolvedNamespace.equals(original.namespace())) {
            return original;
        }
        LOG.info(
                "Resolved @StreamMQDlqConsumer attributes: consumerGroup={} -> {}, namespace={}"
                        + " -> {}",
                original.consumerGroup(),
                resolvedGroup,
                original.namespace(),
                resolvedNamespace);
        InvocationHandler handler =
                (proxy, method, args) -> {
                    String name = method.getName();
                    switch (name) {
                        case "consumerGroup":
                            return resolvedGroup;
                        case "namespace":
                            return resolvedNamespace;
                        default:
                            return method.invoke(original, args);
                    }
                };
        return (StreamMQDlqConsumer)
                Proxy.newProxyInstance(
                        StreamMQDlqConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQDlqConsumer.class},
                        handler);
    }

    /** 扫描 {@code @StreamMQTransactionConsumer} 标注的 Bean，注册到 {@link TransactionScanner}。 */
    private void registerTransactionListeners() {
        TransactionScanner transactionScanner;
        try {
            transactionScanner = applicationContext.getBean(TransactionScanner.class);
        } catch (org.springframework.beans.factory.NoSuchBeanDefinitionException ex) {
            LOG.debug("TransactionScanner not present, skip transaction listener registration");
            return;
        }
        Map<String, Object> beans =
                applicationContext.getBeansWithAnnotation(StreamMQTransactionConsumer.class);
        if (beans.isEmpty()) {
            LOG.info(
                    "No @StreamMQTransactionConsumer beans found, TransactionScanner will have no"
                            + " checkers");
            return;
        }
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            Class<?> targetClass = resolveTargetClass(bean);
            StreamMQTransactionConsumer annotation =
                    AnnotatedElementUtils.findMergedAnnotation(
                            targetClass, StreamMQTransactionConsumer.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式
            StreamMQTransactionConsumer resolved = resolveStreamMQTransactionListener(annotation);
            if (bean instanceof TransactionChecker checker) {
                transactionScanner.registerChecker(resolved.transactionGroup(), checker);
                LOG.info(
                        "Registered TransactionChecker: bean={}, txGroup={}",
                        beanName,
                        resolved.transactionGroup());
            } else {
                LOG.warn(
                        "Bean {} annotated with @StreamMQTransactionConsumer does not implement "
                                + "TransactionChecker, ignored",
                        beanName);
            }
        }
    }

    /**
     * 解析 Bean 的目标类：先剥离 AOP 代理（JDK / CGLIB），再取用户类（去除 CGLIB 子类 $$EnhancerBySpringCGLIB 后缀）。
     *
     * @param bean Bean 实例
     * @return 注解所在的目标类
     */
    private static Class<?> resolveTargetClass(Object bean) {
        return ClassUtils.getUserClass(AopUtils.getTargetClass(bean));
    }

    /** 收集 DLQ 消费者占用的 (topic=group, group) 注册键，参与重复注册预检。 */
    private void collectDlqOccupiedKeys(Map<String, Candidate> candidates) {
        Map<String, Object> dlqBeans =
                applicationContext.getBeansWithAnnotation(StreamMQDlqConsumer.class);
        for (Map.Entry<String, Object> entry : dlqBeans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            Class<?> targetClass = resolveTargetClass(bean);
            StreamMQDlqConsumer annotation =
                    AnnotatedElementUtils.findMergedAnnotation(
                            targetClass, StreamMQDlqConsumer.class);
            if (annotation == null) {
                continue;
            }
            StreamMQDlqConsumer resolved = resolveStreamMQDlqListener(annotation);
            if (!resolved.enable()) {
                continue;
            }
            String key = pairKey(resolved.consumerGroup(), resolved.consumerGroup());
            checkDuplicateRegistration(candidates, key, beanName, targetClass);
            candidates.put(key, new Candidate(beanName, bean, targetClass, resolved));
        }
    }

    /** (topic, group) 注册键。 */
    private static String pairKey(String topic, String group) {
        return topic + ":" + group;
    }

    /**
     * 校验 (topic, group) 组合未被占用，重复时快速失败并点名两个冲突 Bean 类。
     *
     * @param candidates 已收集的候选注册表
     * @param key 待检查的组合键
     * @param beanName 当前 Bean 名
     * @param targetClass 当前 Bean 目标类
     */
    private static void checkDuplicateRegistration(
            Map<String, Candidate> candidates, String key, String beanName, Class<?> targetClass) {
        Candidate existing = candidates.get(key);
        if (existing != null) {
            throw new IllegalStateException(
                    "Duplicate StreamMQ listener registration detected for (topic, group)="
                            + key
                            + ": bean '"
                            + existing.beanName()
                            + "' ("
                            + existing.targetClass().getName()
                            + ") conflicts with bean '"
                            + beanName
                            + "' ("
                            + targetClass.getName()
                            + ")");
        }
    }

    /** 预检阶段的候选注册项快照。 */
    private record Candidate(
            String beanName, Object bean, Class<?> targetClass, Object annotation) {}
}
