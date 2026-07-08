# 快速开始

---

## 环境要求

| 组件 | 最低版本 | 推荐版本 |
|------|----------|----------|
| JDK | 21 | 21 |
| Maven | 3.9 | 3.9+ |
| Redis | 7.2 | 7.2+ |
| Spring Boot | 3.3 | 3.3.5 |

---

## 1. 引入依赖

在 `pom.xml` 中引入 StreamMQ BOM 和 Starter：

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.streammq</groupId>
            <artifactId>streammq-bom</artifactId>
            <version>0.1.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.github.streammq</groupId>
        <artifactId>streammq-spring-boot-starter</artifactId>
    </dependency>

    <dependency>
        <groupId>org.redisson</groupId>
        <artifactId>redisson-spring-boot-starter</artifactId>
    </dependency>
</dependencies>
```

---

## 2. 配置

在 `application.yml` 中配置 StreamMQ 和 Redisson：

```yaml
spring:
  application:
    name: streammq-demo

streammq:
  enabled: true
  namespace: streammq

redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
```

---

## 3. 启用 StreamMQ

在启动类添加 `@EnableStreamMQ` 注解：

```java
@SpringBootApplication
@EnableStreamMQ
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
```

---

## 4. 发送消息

使用 `StreamMessageTemplate` 发送消息：

```java
@Component
public class OrderService {

    private final StreamMessageTemplate template;

    public OrderService(StreamMessageTemplate template) {
        this.template = template;
    }

    public void sendOrder(String orderId, String content) {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic")
                .tag("created")
                .keys(orderId)
                .body(content)
                .userProperty("traceId", "t-001")
                .build();

        SendResult result = template.syncSend(message);
        System.out.println("消息发送成功: " + result.getMessageId());
    }
}
```

---

## 5. 消费消息

使用 `@StreamMQConsumer` 注解定义消费者：

```java
@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        System.out.println("收到订单消息:");
        System.out.println("  - ID: " + message.getKeys());
        System.out.println("  - Tag: " + message.getTag());
        System.out.println("  - Body: " + message.getBody());
        System.out.println("  - 重试次数: " + context.reconsumeTimes());

        // 业务处理
        processOrder(message.getKeys(), message.getBody());

        return ConsumeAction.SUCCESS;
    }

    private void processOrder(String orderId, String content) {
        // 处理订单逻辑
    }
}
```

---

## 6. 测试运行

启动应用后，注入 `OrderService` 并调用发送方法：

```java
@RestController
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/order")
    public String createOrder(@RequestParam String orderId, @RequestParam String content) {
        orderService.sendOrder(orderId, content);
        return "订单消息已发送";
    }
}
```

访问 `http://localhost:8080/order?orderId=123&content=test` 即可发送消息，消费者会自动接收并处理。

---

## 更多示例

- [事务消息示例](../streammq-samples/streammq-sample-transaction)
- [延时消息示例](../streammq-samples/streammq-sample-delay)
- [顺序消息示例](../streammq-samples/streammq-sample-orderly)