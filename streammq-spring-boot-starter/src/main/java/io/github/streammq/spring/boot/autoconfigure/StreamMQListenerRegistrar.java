package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.annotation.AnnotationAttributeResolver;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQTransactionConsumer;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.transaction.TransactionChecker;
import io.github.streammq.spring.boot.support.SpringAnnotationAttributeResolver;
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
 * 在所有单例 Bean 初始化完成后，扫描带有 {@link StreamMQConsumer} /
 * {@link StreamMQTransactionConsumer} 注解的 Bean，将它们注册到
 * {@link DefaultStreamMQListenerContainer} 与 {@link TransactionScanner}。
 *
 * <p>注册顺序：
 * <ol>
 *   <li>扫描 {@code @StreamMQConsumer} 标注的 Bean：
 *     <ul>
 *       <li>若 {@code messageModel = ORDERLY} 且实现 {@link StreamMessageOrderlyConsumer}，注册为顺序消费者</li>
 *       <li>否则若实现 {@link StreamMessageConcurrentlyConsumer}，注册为并发消费者（含 DLQ 场景，由
 *           {@code dlqConsumerGroup} 是否为空区分）</li>
 *     </ul>
 *   </li>
 *   <li>扫描 {@code @StreamMQTransactionConsumer} 标注的 Bean（实现 {@link TransactionChecker}），
 *       注册到 {@link TransactionScanner}</li>
 *   <li>将容器内所有 Listener 的 (topic, group, maxReconsumeTimes) 注册到 RetryScheduler（若存在）</li>
 * </ol>
 *
 * <p>容器的实际启动由 {@code SmartLifecycle} 完成，本类仅负责注册。
 *
 * <p>支持 ${...} 属性占位符与 #{...} SpEL 表达式：在注册前对注解中的
 * topic、consumerGroup、namespace、selectorExpression、dlqConsumerGroup、transactionGroup 等字符串属性进行解析。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQListenerRegistrar implements SmartInitializingSingleton, ApplicationContextAware {

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
     * 创建 {@link StreamMQConsumer} 注解的动态代理，覆盖 topic/consumerGroup/namespace/selectorExpression
     * /dlqConsumerGroup/dlqOriginalGroup 为解析后的值，其余方法委托给原注解。
     *
     * @param original 原注解
     * @return 解析后的代理注解
     */
    private StreamMQConsumer resolveStreamMQListener(StreamMQConsumer original) {
        String resolvedTopic = resolveAttribute(original.topic());
        String resolvedGroup = resolveAttribute(original.consumerGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        String resolvedSelector = resolveAttribute(original.selectorExpression());
        String resolvedDlqConsumerGroup = resolveAttribute(original.dlqConsumerGroup());
        String resolvedDlqOriginalGroup = resolveAttribute(original.dlqOriginalGroup());
        // 若无需解析，直接返回原注解
        if (resolvedTopic.equals(original.topic())
            && resolvedGroup.equals(original.consumerGroup())
            && resolvedNamespace.equals(original.namespace())
            && resolvedSelector.equals(original.selectorExpression())
            && resolvedDlqConsumerGroup.equals(original.dlqConsumerGroup())
            && resolvedDlqOriginalGroup.equals(original.dlqOriginalGroup())) {
            return original;
        }
        LOG.info("Resolved @StreamMQConsumer attributes: topic={} -> {}, consumerGroup={} -> {}, namespace={} -> {}, " +
                "selectorExpression={} -> {}, dlqConsumerGroup={} -> {}, dlqOriginalGroup={} -> {}",
            original.topic(), resolvedTopic,
            original.consumerGroup(), resolvedGroup,
            original.namespace(), resolvedNamespace,
            original.selectorExpression(), resolvedSelector,
            original.dlqConsumerGroup(), resolvedDlqConsumerGroup,
            original.dlqOriginalGroup(), resolvedDlqOriginalGroup);
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            switch (name) {
                case "topic": return resolvedTopic;
                case "consumerGroup": return resolvedGroup;
                case "namespace": return resolvedNamespace;
                case "selectorExpression": return resolvedSelector;
                case "dlqConsumerGroup": return resolvedDlqConsumerGroup;
                case "dlqOriginalGroup": return resolvedDlqOriginalGroup;
                default: return method.invoke(original, args);
            }
        };
        return (StreamMQConsumer) Proxy.newProxyInstance(
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
    private StreamMQTransactionConsumer resolveStreamMQTransactionListener(StreamMQTransactionConsumer original) {
        String resolvedTxGroup = resolveAttribute(original.transactionGroup());
        String resolvedNamespace = resolveAttribute(original.namespace());
        if (resolvedTxGroup.equals(original.transactionGroup())
            && resolvedNamespace.equals(original.namespace())) {
            return original;
        }
        LOG.info("Resolved @StreamMQTransactionConsumer attributes: transactionGroup={} -> {}, namespace={} -> {}",
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
        return (StreamMQTransactionConsumer) Proxy.newProxyInstance(
            StreamMQTransactionConsumer.class.getClassLoader(),
            new Class<?>[] {StreamMQTransactionConsumer.class},
            handler);
    }

    @Override
    public void afterSingletonsInstantiated() {
        registerStreamMQListeners();
        registerTransactionListeners();
        registerRetryTargetsIfPossible();
        LOG.info("StreamMQ listener registration completed, total registrations={}",
            listenerContainer.getConsumers().size());
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
     * 扫描 {@code @StreamMQConsumer} 标注的 Bean，按 {@code messageModel} 与实现的接口区分并发 / 顺序模式，
     * 按 {@code dlqConsumerGroup} 是否为空区分 DLQ 消费者。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void registerStreamMQListeners() {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(StreamMQConsumer.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            StreamMQConsumer annotation = AnnotationUtils.findAnnotation(bean.getClass(), StreamMQConsumer.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式，得到最终的 topic/consumerGroup/namespace/selectorExpression/dlqConsumerGroup
            StreamMQConsumer resolved = resolveStreamMQListener(annotation);
            if (!resolved.enable()) {
                LOG.info("Skip disabled @StreamMQConsumer: bean={}, topic={}", beanName, resolved.topic());
                continue;
            }
            boolean isOrderly = resolved.messageModel() == MessageModel.ORDERLY;
            if (isOrderly && bean instanceof StreamMessageOrderlyConsumer) {
                StreamMessageOrderlyConsumer listener = (StreamMessageOrderlyConsumer) bean;
                listenerContainer.registerOrderlyConsumer(listener, resolved);
                LOG.info("Registered OrderlyConsumer: bean={}, topic={}, group={}",
                    beanName, resolved.topic(), resolved.consumerGroup());
            } else if (bean instanceof StreamMessageConcurrentlyConsumer) {
                StreamMessageConcurrentlyConsumer listener =
                    (StreamMessageConcurrentlyConsumer) bean;
                listenerContainer.registerConsumer(listener, resolved);
                if (resolved.dlqConsumerGroup() != null && !resolved.dlqConsumerGroup().isEmpty()) {
                    LOG.info("Registered DlqConsumer: bean={}, topic={}, originalGroup={}, dlqConsumerGroup={}",
                        beanName, resolved.topic(), resolved.consumerGroup(), resolved.dlqConsumerGroup());
                } else {
                    LOG.info("Registered Consumer: bean={}, topic={}, group={}",
                        beanName, resolved.topic(), resolved.consumerGroup());
                }
            } else {
                LOG.warn("Bean {} annotated with @StreamMQConsumer does not implement " +
                    "StreamMessageConcurrentlyConsumer or StreamMessageOrderlyConsumer, ignored", beanName);
            }
        }
    }

    /**
     * 扫描 {@code @StreamMQTransactionConsumer} 标注的 Bean，注册到 {@link TransactionScanner}。
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
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(StreamMQTransactionConsumer.class);
        if (beans.isEmpty()) {
            LOG.info("No @StreamMQTransactionConsumer beans found, TransactionScanner will have no checkers");
            return;
        }
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            String beanName = entry.getKey();
            Object bean = entry.getValue();
            StreamMQTransactionConsumer annotation = AnnotationUtils.findAnnotation(bean.getClass(),
                StreamMQTransactionConsumer.class);
            if (annotation == null) {
                continue;
            }
            // 解析 ${} 占位符与 #{} SpEL 表达式
            StreamMQTransactionConsumer resolved = resolveStreamMQTransactionListener(annotation);
            if (bean instanceof TransactionChecker) {
                TransactionChecker checker = (TransactionChecker) bean;
                transactionScanner.registerChecker(resolved.transactionGroup(), checker);
                LOG.info("Registered TransactionChecker: bean={}, txGroup={}",
                    beanName, resolved.transactionGroup());
            } else {
                LOG.warn("Bean {} annotated with @StreamMQTransactionConsumer does not implement " +
                    "TransactionChecker, ignored", beanName);
            }
        }
    }
}
