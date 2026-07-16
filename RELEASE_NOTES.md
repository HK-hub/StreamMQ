# StreamMQ v0.1.0 Release Notes

**让 Redis 成为你的消息总线**

StreamMQ 是一个开源的轻量级消息中间件 SDK，基于 Redis Stream + Redisson 实现，提供 RocketMQ 风格的 API 和开发体验。无需部署独立 MQ 集群，只需复用现有 Redis 即可获得企业级消息特性。

---

## 核心特性

### 消息生产者
- `StreamMessageTemplate` 编程式发送 API
- 支持同步 / 异步 / 单向 / 批量发送
- 事务消息（半消息 + 本地事务 + 回查机制）
- 18 级固定延迟消息 + 任意毫秒级延迟消息
- 顺序消息（基于 Sharding Key 分区）
- 消息压缩（Snappy / Gzip / LZ4）

### 消息消费者
- `@StreamMQConsumer` 注解驱动消费
- 并发消费 / 顺序消费 / 广播消费
- 死信队列（DLQ）+ 二级 DLQ
- 消费失败策略（重试 / 转 DLQ / 丢弃）
- 背压控制（队列积压限制）

### 消息过滤
- Tag 表达式过滤
- SQL92 表达式过滤

### 可扩展性（12 个 SPI 扩展点）
- 序列化器 / 转换器 / 过滤器 / 拦截器
- 重试策略 / 重平衡策略 / 压缩编解码器
- 追踪收集器 / 管理认证器 / DLQ 失败策略

### 可观测性
- Micrometer 指标集成
- Spring Boot Actuator 健康端点与指标
- 管理端 REST API
- Trace 上下文传播（MDC）

### 零侵入集成
- Spring Boot 3 自动配置
- 配置属性绑定（`application.yml`）
- 开箱即用，极少配置

---

## 模块结构
| 模块 | 说明 |
|---|---|
| `streammq-core` | 核心抽象层（消息模型 / API 接口 / SPI） |
| `streammq-redisson` | Redisson 适配器（Redis Stream 实现） |
| `streammq-spring-boot-starter` | Spring Boot 3 自动配置 / Actuator |
| `streammq-test` | 测试工具集（嵌入式 Redis 等） |
| `streammq-samples` | 示例项目（快速入门 / 事务 / 延迟 / 顺序 / DLQ / 拦截器） |
| `streammq-bom` | BOM 依赖管理 |

---

## 环境要求
- **Java**: 21+
- **Spring Boot**: 3.3.x
- **Redis**: 5.0+ (需支持 Stream)
- **Maven**: 3.8+

---

## 快速开始

```xml
<dependency>
    <groupId>io.github.streammq</groupId>
    <artifactId>streammq-spring-boot-starter</artifactId>
    <version>0.1.0</version>
</dependency>
```

```yaml
streammq:
  redis:
    address: redis://localhost:6379
  consumer:
    group: my-consumer-group
```

```java
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-group")
public void onOrder(Message message) {
    // 处理消息
}
```

---

## 许可证
MIT License

---

## 参与贡献
欢迎提交 Issue / PR！请参阅 [CONTRIBUTING.md](./CONTRIBUTING.md)（如有）了解详情。
