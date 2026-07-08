# StreamMQ

> 基于 Redis Stream + Redisson 的高性能消息中间件 SDK，让 Redis 成为你的消息总线。

---

## 什么是 StreamMQ

StreamMQ 是一款基于 **Redis Stream** 与 **Redisson** 构建的开源消息中间件 SDK，以 MIT 协议发布。它将 Redis Stream 的原生能力封装为一套类似 RocketMQ 的、面向业务开发者友好的消息 API。

如果你已经在使用 Redis，又需要一个轻量、可靠、易用的消息中间件，StreamMQ 是你的理想选择。

---

## 核心优势

### 零额外部署

无需引入独立 MQ 集群，复用现有 Redis 基础设施即可获得完整消息中间件能力。

### 注解驱动

`@StreamMQConsumer` 声明式定义消费者，一行代码即可开启消息监听。

### 丰富特性

事务消息、延时消息、顺序消息、批量发送等高级特性开箱即用。

### Spring Boot 3 深度集成

自动装配、配置绑定、Actuator 端点，与 Spring 生态无缝衔接。

---

## 快速开始

```java
// 1. 发送消息
@Autowired
private StreamMessageTemplate template;

template.syncSend(MessageBuilder.<String>withTopic("order-topic")
        .tag("created")
        .body("Hello StreamMQ")
        .build());

// 2. 消费消息
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {
    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        System.out.println("收到消息: " + message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
```

[查看完整快速开始 →](quickstart.md)

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **事务消息** | 半消息 + 本地事务 + 回查机制 |
| **延时消息** | 18 级固定延时 + 任意毫秒延时 |
| **顺序消息** | ShardingKey 分片顺序消费 |
| **批量发送** | Pipeline 批量投递 |
| **背压控制** | InflightQueue 拉取-处理解耦 |
| **消费超时** | 超时自动取消并重试 |
| **死信队列** | 重试耗尽后进入 DLQ |
| **可观测性** | Micrometer 指标 + MDC 日志 |

[查看全部特性 →](features.md)

---

## 适用场景

- 已有 Redis 基础设施，希望复用为消息总线
- 中小规模业务（单集群日消息量 < 1 亿）
- 需要事务消息 / 延时消息 / 顺序消息能力
- 微服务架构下的轻量级异步通信

---

## 开始使用

1. [快速开始](quickstart.md) - 5 分钟上手
2. [核心概念](concepts.md) - 理解关键术语
3. [API 文档](api.md) - 完整 API 参考
4. [配置参考](configuration.md) - 全部配置项

---

## 版本

当前版本：**0.1.0-SNAPSHOT**

---

*StreamMQ · 让 Redis 成为你的消息总线。*