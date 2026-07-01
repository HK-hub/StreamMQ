package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.container.DefaultStreamMqListenerContainer;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.annotation.StreamMqListener;
import io.github.streammq.core.annotation.StreamMqOrderlyListener;
import io.github.streammq.core.annotation.StreamMqTransactionListener;
import io.github.streammq.core.listener.StreamMqAckListener;
import io.github.streammq.core.transaction.TransactionChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;

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
 *   <li>扫描 {@code @StreamMqTransactionListener} 标注的 Bean（实现 {@link TransactionChecker}），
 *       注册到 {@link TransactionScanner}</li>
 *   <li>将容器内所有 Listener 的 (topic, group, maxReconsumeTimes) 注册到 RetryScheduler（若存在）</li>
 * </ol>
 *
 * <p>容器的实际启动由 {@code SmartLifecycle} 完成，本类仅负责注册。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMqListenerRegistrar implements SmartInitializingSingleton, ApplicationContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqListenerRegistrar.class);

    private final DefaultStreamMqListenerContainer listenerContainer;
    private ApplicationContext applicationContext;

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
    }

    @Override
    public void afterSingletonsInstantiated() {
        registerStreamMqListeners();
        registerOrderlyListeners();
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
            ((DefaultStreamMqListenerContainer) listenerContainer).registerRetryTargets(retryScheduler);
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
            if (!annotation.enable()) {
                LOG.info("Skip disabled @StreamMqListener: bean={}, topic={}", beanName, annotation.topic());
                continue;
            }
            if (bean instanceof StreamMqAckListener) {
                StreamMqAckListener listener = (StreamMqAckListener) bean;
                listenerContainer.registerAckListener(listener, annotation);
                LOG.info("Registered AckListener: bean={}, topic={}, group={}",
                    beanName, annotation.topic(), annotation.consumerGroup());
            } else if (bean instanceof io.github.streammq.core.listener.StreamMqListener) {
                io.github.streammq.core.listener.StreamMqListener listener =
                    (io.github.streammq.core.listener.StreamMqListener) bean;
                listenerContainer.registerListener(listener, annotation);
                LOG.info("Registered Listener: bean={}, topic={}, group={}",
                    beanName, annotation.topic(), annotation.consumerGroup());
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
            if (!annotation.enable()) {
                LOG.info("Skip disabled @StreamMqOrderlyListener: bean={}, topic={}",
                    beanName, annotation.topic());
                continue;
            }
            if (bean instanceof io.github.streammq.core.listener.StreamMqOrderlyListener) {
                io.github.streammq.core.listener.StreamMqOrderlyListener listener =
                    (io.github.streammq.core.listener.StreamMqOrderlyListener) bean;
                listenerContainer.registerOrderlyListener(listener, annotation);
                LOG.info("Registered OrderlyListener: bean={}, topic={}, group={}",
                    beanName, annotation.topic(), annotation.consumerGroup());
            } else {
                LOG.warn("Bean {} annotated with @StreamMqOrderlyListener does not implement " +
                    "StreamMqOrderlyListener, ignored", beanName);
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
            if (bean instanceof TransactionChecker) {
                TransactionChecker checker = (TransactionChecker) bean;
                transactionScanner.registerChecker(annotation.transactionGroup(), checker);
                LOG.info("Registered TransactionChecker: bean={}, txGroup={}",
                    beanName, annotation.transactionGroup());
            } else {
                LOG.warn("Bean {} annotated with @StreamMqTransactionListener does not implement " +
                    "TransactionChecker, ignored", beanName);
            }
        }
    }
}
