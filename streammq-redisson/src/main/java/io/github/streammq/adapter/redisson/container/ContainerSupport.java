/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

/** 容器内部共享的小型工具方法。 */
final class ContainerSupport {

    private ContainerSupport() {}

    /** 可中断休眠（吞掉中断并恢复中断标志，供消费循环退避使用）。 */
    static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
