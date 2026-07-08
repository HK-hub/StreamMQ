# FAQ

---

## 常见问题

### Q1: StreamMQ 与 RocketMQ 有什么区别？

**A:** StreamMQ 基于 Redis Stream 实现，部署简单（仅需 Redis），适合中小规模场景；RocketMQ 需要独立集群部署，适合大规模场景。StreamMQ 的 API 设计对齐 RocketMQ，学习成本低。

### Q2: StreamMQ 支持哪些消息类型？

**A:** 支持普通消息、事务消息、延时消息、顺序消息、批量消息。

### Q3: 延时消息的精度如何？

**A:** 延时消息基于 Redis ZSet 实现，精度受轮询间隔影响，默认轮询间隔为 1 秒，实际精度在毫秒级别。

### Q4: 顺序消息如何保证顺序？

**A:** 基于 ShardingKey 分片，相同 shardingKey 的消息路由到同一分片，同一分片内单线程串行消费，保证顺序。

### Q5: 如何处理消息重复消费？

**A:** 建议在业务层面实现幂等性，使用 message.getKeys() 或 message.getMessageId() 作为幂等键。

### Q6: StreamMQ 是否支持消息持久化？

**A:** 支持，基于 Redis Stream 的持久化机制，Redis 开启 appendonly 后消息会持久化到磁盘。

### Q7: 如何监控消息队列状态？

**A:** 通过 Actuator 端点和 Micrometer 指标监控，支持 Prometheus 集成。

### Q8: 死信队列的消息如何处理？

**A:** 实现 DLQ 消费者处理死信消息，可以记录日志、告警通知或人工干预。

### Q9: 如何调整消费线程数？

**A:** 通过 `@StreamMQConsumer` 注解的 `consumeThreadMin` 和 `consumeThreadMax` 属性配置。

### Q10: 如何实现消息过滤？

**A:** 通过 `selectorExpression` 属性配置 Tag 过滤表达式，支持 `*`、`||`、`&&` 操作符。

---

## 故障排查

### 消息发送失败

**原因：** Redis 连接异常、网络问题、超时

**排查：**
1. 检查 Redis 服务是否正常运行
2. 检查网络连通性
3. 查看日志中的错误信息
4. 检查 Redis 内存使用情况

### 消息消费失败

**原因：** 业务异常、消费超时、重试耗尽

**排查：**
1. 查看消费者日志
2. 检查 `context.reconsumeTimes()` 重试次数
3. 检查消费超时配置
4. 检查 DLQ 中是否有消息

### 消息堆积

**原因：** 消费速度慢于生产速度、消费者数量不足、业务处理耗时过长

**排查：**
1. 增加消费线程数
2. 增加消费者实例数
3. 优化业务处理逻辑
4. 检查背压队列配置

### 事务消息回查频繁

**原因：** 本地事务返回 UNKNOW、网络抖动

**排查：**
1. 检查本地事务逻辑
2. 检查网络稳定性
3. 调整回查间隔配置

---

## 性能优化

### 发送性能优化

1. 使用 `sendOneway` 发送不需要确认的消息
2. 使用 `syncSendBatch` 批量发送
3. 调整 Redis Pipeline 配置
4. 增加 Redis 连接池大小

### 消费性能优化

1. 增加消费线程数
2. 调整 `pullBatchSize` 参数
3. 优化业务处理逻辑
4. 使用虚拟线程（JDK 21+）

### Redis 优化

1. 开启 Redis 持久化
2. 配置合理的 maxmemory-policy
3. 使用 Redis Cluster 提高吞吐量
4. 定期清理过期数据

---

## 迁移指南

### 从 RocketMQ 迁移

1. 替换 Maven 依赖
2. 修改 `@RocketMQMessageListener` 为 `@StreamMQConsumer`
3. 修改 `RocketMQTemplate` 为 `StreamMessageTemplate`
4. 调整消息构建方式

### 从 Kafka 迁移

1. 使用 `streammq-kafka-compat` 模块
2. 修改 `KafkaProducer` 为兼容层的 `KafkaProducer`
3. 修改 `KafkaConsumer` 为兼容层的 `KafkaConsumer`

### 从 RabbitMQ 迁移

1. 使用 `streammq-amqp-compat` 模块
2. 修改 `AmqpClient` 为兼容层的 `AmqpClient`
3. 调整 Exchange/Queue/RoutingKey 映射