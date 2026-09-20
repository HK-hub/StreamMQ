/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.listener;

/**
 * 容器在途消息计数可选契约（K2）。
 *
 * <p>{@link StreamMQListenerContainer} 的公共 API 未暴露 in-flight 计数，优雅关闭流程因此无法判断「处理中的消息是否已排空」。 容器实现（如
 * {@code streammq-redisson} 的 {@code DefaultStreamMQListenerContainer}）可实现本接口提供真实判据：关闭处理器（如
 * kubernetes 的 {@code GracefulShutdownHandler}）在 pause 之后按有界轮询等待计数归零并提前退出，而不是无条件睡满 {@code
 * graceful-shutdown-timeout-ms}。
 *
 * <p><b>为什么契约定义在 core：</b>本接口是「容器实现方」与「关闭流程编排方」的公共契约，双方不得互相依赖（redisson 等适配器模块不能依赖 kubernetes
 * 等上层集成模块）。契约必须落在所有容器实现都可依赖的 core 模块，否则容器侧无法实现、 关闭侧的 instanceof 判据永远是死代码。
 *
 * <p><b>计数口径（实现方必须遵守）：</b>只统计<b>当前正在执行 handler</b> 的消息条数——即消息已进入消费管线、尚未完成 ACK/NACK
 * 路由；<b>不包含</b>排队待处理、重试 ZSet 中、PEL 中滞留的消息。计数只用于关闭期收敛判据，读值允许为近似值 （上界偏差可接受），但实现必须并发安全，且<b>禁止抛异常</b>。
 *
 * <p>未实现本接口的容器：关闭流程跳过 grace 空转，把在途排空交给 {@code container.stop()}（redisson 容器内部会 awaitTermination
 * 排空消费线程）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
public interface InFlightAware {

    /**
     * 返回当前正在执行 handler（已进入消费管线、尚未完成 ACK/NACK 路由）的消息数。
     *
     * @return in-flight 消息数，不小于 0；无法统计时返回最接近的真实估计值，禁止抛异常
     */
    int getInFlightCount();
}
