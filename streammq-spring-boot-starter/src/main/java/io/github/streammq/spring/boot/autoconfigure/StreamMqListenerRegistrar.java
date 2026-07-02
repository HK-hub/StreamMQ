package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMqListenerContainer;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.annotation.StreamMqListener;
import io.github.streammq.core.annotation.StreamMqDlqListener;
import io.github.streammq.core.annotation.StreamMqOrderlyListener;
import io.github.streammq.core.annotation.StreamMqTransactionListener;
import io.github.streammq.core.listener.StreamMqAckListener;
import io.github.streammq.core.transaction.TransactionChecker;
import io.github.streammq.spring.boot.support.AnnotationAttributeResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * 在所有单例 Bean 初始化完成后，扫描带有 {@link StreamMqListener} / {@link StreamMqOrderlyListener}
 * / {@link StreamMqTransactionListener} 注解的 Bean，将它们注册到
 * {@link DefaultStreamMqListenerContainer} 与 {@link TransactionScanner}。
 *
 * <p>注册顺序：
 * <ol>
 *   <li>扫描 {@code @StreamMqListener} 标注的 Bean（实现 {@link io.github.streammq.core.listener.StreamMqListener}
 *       或 {@link StreamMqAckListener}）</li>
 *   <li>扫描 {@code @StreamMqOrderlyListener} 标注的 Bean（实现 {@link StreamMqOrderlyListener}）</li>
 *   <li>扫描 {@code @StreamMqDlqListener} 标注的 Bean（实现 {@link io.github.streammq.core.listener.StreamMqListener}），
 *       注册为 DLQ 消费者</li>
 *   <li>扫描 {@code @StreamMqTransactionListener} 标注的 Bean（实现 {@link TransactionChecker}），
 *       注册到 {@link TransactionScanner}</li>
 *   <li>将容器内所有 Listener 的 (topic, group, maxReconsumeTimes) 注册到 RetryScheduler（若存在）</li>
 * </ol>
 *
 * <p>容器的实际启动由 {@code SmartLifecycle} 完成，本类仅负责注册。
 *
 * <p>支持 ${...} 属性占位符与 #{...} SpEL 表达式：在注册前对注解中的
 * topic、consumerGroup、namespace、selectorExpression、transactionGroup 等字符串属性进行解析。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMqListenerRegistrar implements SmartInitializingSingleton, ApplicationContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqListenerRegistrar.class);

    private final DefaultStreamMqListenerContainer listenerContainer;
    private ApplicationContext applicationContext;
    /** 注解属性解析器（支持 ${} 占位符与 #{} SpEL 表达式） */
    private AnnotationAttributeResolver attributeResolver;

    /**
     * 构造 Registrar。
     *
     * @param listenerContainer Listener 容器
     */
    public StreamMqListenerRegistrar(DefaultStreamMqListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        // 初始化注解属性解析器（支持 ${} 占位符与 #{} SpEL 表达式）
        if (applicationContext instanceof ConfigurableApplicationContext cac) {
            this.attributeResolver = new AnnotationAttributeResolver(cac);
        }
    }

    /**
     * 解析字符串属性值（支持 ${} 占位符与 #{} SpEL 表达式）。
     * 若 attributeResolver 未初始化，则返回原值。
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
     * 创建 {@link StreamMqListener} 注解的动态代理，覆盖 topic/consumerGroup/namespace/selectorExpression
     * 为解析后的值，其余方法委托给原注解。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMqListener resolveStreamMqListener(StreamMqListener original) {
        String resolvedTopic = resolveAttribute(original.topic());
        String resolvedGroup = resolveAttribute(original.consumerGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        String resolvedSelector = resolveAttribute(original.selectorExpression());
        // 若无需解析，直接返回原注解
        if (resolvedTopic.equals(original.topic())
            && resolvedGroup.equals(original.consumerGroup())
            && resolvedNamespace.equals(original.namespace())
            && resolvedSelector.equals(original.selectorExpression())) {
            return original;
        }
        LOG.info("Resolved @StreamMqListener attributes: topic={} -> {}, consumerGroup={} -> {}, namespace={} -> {}, selectorExpression={} -> {}",
            original.topic(), resolvedTopic,
            original.consumerGroup(), resolvedGroup,
            original.namespace(), resolvedNamespace,
            original.selectorExpression(), resolvedSelector);
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            switch (name) {
                case "topic": return resolvedTopic;
                case "consumerGroup": return resolvedGroup;
                case "namespace": return resolvedNamespace;
                case "selectorExpression": return resolvedSelector;
                default: return method.invoke(original, args);
            }
        };
        return (StreamMqListener) Proxy.newProxyInstance(
            StreamMqListener.class.getClassLoader(),
            new Class<?>[] {StreamMqListener.class},
            handler);
    }

    /**
     * 创建 {@link StreamMqOrderlyListener} 注解的动态代理，覆盖 topic/consumerGroup/namespace/selectorExpression
     * 为解析后的值，其余方法委托给原注解。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMqOrderlyListener resolveStreamMqOrderlyListener(StreamMqOrderlyListener original) {
        String resolvedTopic = resolveAttribute(original.topic());
        String resolvedGroup = resolveAttribute(original.consumerGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        String resolvedSelector = resolveAttribute(original.selectorExpression());
        if (resolvedTopic.equals(original.topic())
            && resolvedGroup.equals(original.consumerGroup())
            && resolvedNamespace.equals(original.namespace())
            && resolvedSelector.equals(original.selectorExpression())) {
            return original;
        }
        LOG.info("Resolved @StreamMqOrderlyListener attributes: topic={} -> {}, consumerGroup={} -> {}, namespace={} -> {}, selectorExpression={} -> {}",
            original.topic(), resolvedTopic,
            original.consumerGroup(), resolvedGroup,
            original.namespace(), resolvedNamespace,
            original.selectorExpression(), resolvedSelector);
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            switch (name) {
                case "topic": return resolvedTopic;
                case "consumerGroup": return resolvedGroup;
                case "namespace": return resolvedNamespace;
                case "selectorExpression": return resolvedSelector;
                default: return method.invoke(original, args);
            }
        };
        return (StreamMqOrderlyListener) Proxy.newProxyInstance(
            StreamMqOrderlyListener.class.getClassLoader(),
            new Class<?>[] {StreamMqOrderlyListener.class},
            handler);
    }

    /**
     * 创建 {@link StreamMqTransactionListener} 注解的动态代理，覆盖 transactionGroup/namespace
     * 为解析后的值，其余方法委托给原注解。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMqTransactionListener resolveStreamMqTransactionListener(StreamMqTransactionListener original) {
        String resolvedTxGroup = resolveAttribute(original.transactionGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        if (resolvedTxGroup.equals(original.transactionGroup())
            && resolvedNamespace.equals(original.namespace())) {
            return original;
        }
        LOG.info("Resolved @StreamMqTransactionListener attributes: transactionGroup={} -> {}, namespace={} -> {}",
            original.transactionGroup(), resolvedTxGroup,
            original.namespace(), resolvedNamespace);
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            switch (name) {
                case "transactionGroup": return resolvedTxGroup;
                case "namespace": return resolvedNamespace;
                default: return method.invoke(original, args);
            }
        };
        return (StreamMqTransactionListener) Proxy.newProxyInstance(
            StreamMqTransactionListener.class.getClassLoader(),
            new Class<?>[] {StreamMqTransactionListener.class},
            handler);
    }

    /**
     * 创建 {@link StreamMqDlqListener} 注解的动态代理，覆盖 topic/consumerGroup/dlqConsumerGroup/namespace
     * 为解析后的值，其余方法委托给原注解。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMqDlqListener resolveStreamMqDlqListener(StreamMqDlqListener original) {
        String resolvedTopic = resolveAttribute(original.topic());
        String resolvedGroup = resolveAttribute(original.consumerGroup());
        String resolvedDlqGroup = resolveAttribute(original.dlqConsumerGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        if (resolvedTopic.equals(original.topic())
            && resolvedGroup.equals(original.consumerGroup())
            && resolvedDlqGroup.equals(original.dlqConsumerGroup())
            && resolvedNamespace.equals(original.namespace())) {
            return original;
        }
        LOG.info("Resolved @StreamMqDlqListener attributes: topic={} -> {}, consumerGroup={} -> {}, " +
                "dlqConsumerGroup={} -> {}, namespace={} -> {}",
            original.topic(), resolvedTopic,
            original.consumerGroup(), resolvedGroup,
            original.dlqConsumerGroup(), resolvedDlqGroup,
            original.namespace(), resolvedNamespace);
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            switch (name) {
                case "topic": return resolvedTopic;
                case "consumerGroup": return resolvedGroup;
                case "dlqConsumerGroup": return resolvedDlqGroup;
                case "namespace": return resolvedNamespace;
                default: return method.invoke(original, args);
            }
        };
        return (StreamMqDlqListener) Proxy.newProxyInstance(
            StreamMqDlqListener.class.getClassLoader(),
            new Class<?>[] {StreamMqDlqListener.class},
            handler);
    }

    @Override
    public void afterSingletonsInstantiated() {
        registerStreamMqListeners();
        registerOrderlyListeners();
        registerDlqListeners();
        registerTransactionListeners();
        registerRetryTargetsIfPossible();
        LOG.info("StreamMq listener registration completed, total registrations={}",
            listenerContainer.getListeners().size());
    }

    /**
     * 若 RetryScheduler 已注册，将容器内所有 (topic, group, maxReconsumeTimes) 注册到 RetryScheduler。
     */
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
     * 扫描 {@code @StreamMqListener} 标注的 Bean，按实现的接口区分并发 / 手动 ACK 模式。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void registerStreamMqListeners() {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(StreamMqListener.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            StreamMqListener annotation = AnnotationUtils.findAnnotation(bean.getClass(), StreamMqListener.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式，得到最终的 topic/consumerGroup/namespace/selectorExpression
            StreamMqListener resolved = resolveStreamMqListener(annotation);
            if (!resolved.enable()) {
                LOG.info("Skip disabled @StreamMqListener: bean={}, topic={}", beanName, resolved.topic());
                continue;
            }
            if (bean instanceof StreamMqAckListener) {
                StreamMqAckListener listener = (StreamMqAckListener) bean;
                listenerContainer.registerAckListener(listener, resolved);
                LOG.info("Registered AckListener: bean={}, topic={}, group={}",
                    beanName, resolved.topic(), resolved.consumerGroup());
            } else if (bean instanceof io.github.streammq.core.listener.StreamMqListener) {
                io.github.streammq.core.listener.StreamMqListener listener =
                    (io.github.streammq.core.listener.StreamMqListener) bean;
                listenerContainer.registerListener(listener, resolved);
                LOG.info("Registered Listener: bean={}, topic={}, group={}",
                    beanName, resolved.topic(), resolved.consumerGroup());
            } else {
                LOG.warn("Bean {} annotated with @StreamMqListener does not implement StreamMqListener " +
                    "or StreamMqAckListener, ignored", beanName);
            }
        }
    }

    /**
     * 扫描 {@code @StreamMqOrderlyListener} 标注的 Bean。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void registerOrderlyListeners() {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(StreamMqOrderlyListener.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            StreamMqOrderlyListener annotation = AnnotationUtils.findAnnotation(bean.getClass(), StreamMqOrderlyListener.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式
            StreamMqOrderlyListener resolved = resolveStreamMqOrderlyListener(annotation);
            if (!resolved.enable()) {
                LOG.info("Skip disabled @StreamMqOrderlyListener: bean={}, topic={}",
                    beanName, resolved.topic());
                continue;
            }
            if (bean instanceof io.github.streammq.core.listener.StreamMqOrderlyListener) {
                io.github.streammq.core.listener.StreamMqOrderlyListener listener =
                    (io.github.streammq.core.listener.StreamMqOrderlyListener) bean;
                listenerContainer.registerOrderlyListener(listener, resolved);
                LOG.info("Registered OrderlyListener: bean={}, topic={}, group={}",
                    beanName, resolved.topic(), resolved.consumerGroup());
            } else {
                LOG.warn("Bean {} annotated with @StreamMqOrderlyListener does not implement " +
                    "StreamMqOrderlyListener, ignored", beanName);
            }
        }
    }

    /**
     * 扫描 {@code @StreamMqDlqListener} 标注的 Bean，注册为 DLQ 消费者。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void registerDlqListeners() {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(StreamMqDlqListener.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            StreamMqDlqListener annotation = AnnotationUtils.findAnnotation(bean.getClass(), StreamMqDlqListener.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式
            StreamMqDlqListener resolved = resolveStreamMqDlqListener(annotation);
            if (!resolved.enable()) {
                LOG.info("Skip disabled @StreamMqDlqListener: bean={}, topic={}",
                    beanName, resolved.topic());
                continue;
            }
            if (bean instanceof io.github.streammq.core.listener.StreamMqListener) {
                io.github.streammq.core.listener.StreamMqListener listener =
                    (io.github.streammq.core.listener.StreamMqListener) bean;
                listenerContainer.registerDlqListener(listener, resolved);
                LOG.info("Registered DlqListener: bean={}, topic={}, originalGroup={}",
                    beanName, resolved.topic(), resolved.consumerGroup());
            } else {
                LOG.warn("Bean {} annotated with @StreamMqDlqListener does not implement " +
                    "StreamMqListener, ignored", beanName);
            }
        }
    }

    /**
     * 扫描 {@code @StreamMqTransactionListener} 标注的 Bean，注册到 {@link TransactionScanner}。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void registerTransactionListeners() {
        TransactionScanner transactionScanner;
        try {
            transactionScanner = applicationContext.getBean(TransactionScanner.class);
        } catch (org.springframework.beans.factory.NoSuchBeanDefinitionException ex) {
            LOG.debug("TransactionScanner not present, skip transaction listener registration");
            return;
        }
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(StreamMqTransactionListener.class);
        if (beans.isEmpty()) {
            LOG.info("No @StreamMqTransactionListener beans found, TransactionScanner will have no checkers");
            return;
        }
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            StreamMqTransactionListener annotation = AnnotationUtils.findAnnotation(bean.getClass(),
                StreamMqTransactionListener.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式
            StreamMqTransactionListener resolved = resolveStreamMqTransactionListener(annotation);
            if (bean instanceof TransactionChecker) {
                TransactionChecker checker = (TransactionChecker) bean;
                transactionScanner.registerChecker(resolved.transactionGroup(), checker);
                LOG.info("Registered TransactionChecker: bean={}, txGroup={}",
                    beanName, resolved.transactionGroup());
            } else {
                LOG.warn("Bean {} annotated with @StreamMqTransactionListener does not implement " +
                    "TransactionChecker, ignored", beanName);
            }
        }
    }
}
