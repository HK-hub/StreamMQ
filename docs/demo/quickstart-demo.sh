#!/usr/bin/env bash
#
# StreamMQ 快速开始演示脚本
# 演示从零开始使用 StreamMQ 的完整流程
#
# 前提条件：
#   - JDK 21+
#   - Maven 3.9+
#   - Redis 7.2+ (运行在 localhost:6379)
#
# 用法：
#   bash quickstart-demo.sh

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$DEMO_DIR/../.." && pwd)"
TEMP_DIR="/tmp/streammq-demo"

check_prerequisites() {
    log_step "检查前置条件..."

    if ! command -v java &>/dev/null; then
        log_error "未找到 java，请安装 JDK 21+"
        exit 1
    fi
    JAVA_VERSION=$(java -version 2>&1 | head -1 | awk -F '"' '{print $2}')
    log_info "JDK 版本: $JAVA_VERSION"

    if ! command -v mvn &>/dev/null; then
        log_error "未找到 mvn，请安装 Maven 3.9+"
        exit 1
    fi
    MVN_VERSION=$(mvn --version 2>&1 | head -1 | awk '{print $3}')
    log_info "Maven 版本: $MVN_VERSION"

    if ! command -v redis-cli &>/dev/null; then
        log_warn "未找到 redis-cli，将跳过 Redis 连接测试"
    else
        if redis-cli ping &>/dev/null; then
            log_info "Redis 连接正常"
        else
            log_warn "Redis 未运行，将在后台启动..."
            start_redis
        fi
    fi
}

start_redis() {
    log_step "启动 Redis..."
    if command -v redis-server &>/dev/null; then
        redis-server --daemonize yes
        sleep 1
        if redis-cli ping &>/dev/null; then
            log_info "Redis 已启动"
        else
            log_error "Redis 启动失败"
            exit 1
        fi
    else
        log_warn "未找到 redis-server，请手动启动 Redis 7.2+"
        log_warn "Docker 方式: docker run -d --name redis -p 6379:6379 redis:7.2"
    fi
}

create_demo_app() {
    log_step "创建演示 Spring Boot 应用..."

    rm -rf "$TEMP_DIR"
    mkdir -p "$TEMP_DIR/src/main/java/com/example/demo"
    mkdir -p "$TEMP_DIR/src/main/resources"

    # 创建 pom.xml
    cat > "$TEMP_DIR/pom.xml" << 'MAVEN_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>streammq-demo</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <name>StreamMQ Demo</name>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.5</version>
        <relativePath/>
    </parent>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>io.github.streammq</groupId>
                <artifactId>streammq-bom</artifactId>
                <version>0.1.0</version>
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
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
MAVEN_EOF

    # 创建 application.yml
    cat > "$TEMP_DIR/src/main/resources/application.yml" << 'YAML_EOF'
spring:
  application:
    name: streammq-demo

streammq:
  enabled: true
  namespace: demo

redisson:
  singleServerConfig:
    address: "redis://127.0.0.1:6379"
    database: 0
YAML_EOF

    # 创建启动类
    cat > "$TEMP_DIR/src/main/java/com/example/demo/DemoApplication.java" << 'JAVA_EOF'
package com.example.demo;

import io.github.streammq.core.annotation.EnableStreamMQ;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableStreamMQ
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
JAVA_EOF

    # 创建启动即发送演示消息的 Runner（应用就绪后自动发送一条演示消息）
    cat > "$TEMP_DIR/src/main/java/com/example/demo/DemoRunner.java" << 'JAVA_EOF'
package com.example.demo;

import io.github.streammq.core.message.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * 应用启动完成后自动发送一条演示消息，形成「发送 → 消费」闭环。
 * 对齐 streammq-sample-quickstart 的 DemoRunner 模式。
 */
@Component
public class DemoRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoRunner.class);

    private final OrderService orderService;

    public DemoRunner(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("DemoRunner: 发送演示消息...");
        SendResult result = orderService.sendOrder("demo-001", "Hello StreamMQ");
        log.info("DemoRunner: 演示消息已发送, result={}", result);
    }
}
JAVA_EOF

    # 创建消息发送 Service
    cat > "$TEMP_DIR/src/main/java/com/example/demo/OrderService.java" << 'JAVA_EOF'
package com.example.demo;

import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderService {

    private final StreamMessageTemplate template;

    public OrderService(StreamMessageTemplate template) {
        this.template = template;
    }

    public SendResult sendOrder(String orderId, String content) {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic")
                .tag("created")
                .keys(orderId)
                .body(content)
                .withUserProperty("traceId", "demo-" + orderId)
                .build();
        return template.syncSend(message);
    }
}
JAVA_EOF

    # 创建消息消费者
    cat > "$TEMP_DIR/src/main/java/com/example/demo/OrderConsumer.java" << 'JAVA_EOF'
package com.example.demo;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@StreamMQConsumer(topic = "order-topic", consumerGroup = "order-consumer-group")
public class OrderConsumer implements StreamMessageConcurrentlyConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
        // 「收到消息」为演示脚本的轮询标记行，请勿删除
        log.info("[StreamMQ Demo] 收到消息: keys={}, tag={}, body={}",
                message.getKeys(), message.getTag(), message.getBody());
        return ConsumeAction.SUCCESS;
    }
}
JAVA_EOF

    log_info "演示应用已创建到: $TEMP_DIR"
}

build_and_run() {
    log_step "构建演示应用..."

    cd "$TEMP_DIR"
    mvn clean compile -q

    log_info "构建成功"

    log_step "启动演示应用..."
    log_info "等待应用启动（约 10 秒）..."

    # 应用日志重定向到文件：供消费检测轮询与失败诊断使用
    mvn spring-boot:run > "$TEMP_DIR/app.log" 2>&1 &
    APP_PID=$!

    sleep 10

    if kill -0 "$APP_PID" 2>/dev/null; then
        log_info "应用已启动 (PID: $APP_PID)，日志: $TEMP_DIR/app.log"
    else
        log_error "应用启动失败，请检查日志"
        tail -n 50 "$TEMP_DIR/app.log" || true
        exit 1
    fi
}

wait_for_consumption() {
    log_step "等待消费者接收演示消息（最长 60 秒）..."
    log_info "演示应用启动后已自动发送一条消息（DemoRunner），此处轮询应用日志确认消费。"

    local waited=0
    while [ "$waited" -lt 60 ]; do
        if grep -q "收到消息" "$TEMP_DIR/app.log" 2>/dev/null; then
            log_info "消费者已成功接收演示消息 ✓"
            return 0
        fi
        sleep 2
        waited=$((waited + 2))
    done

    log_error "超时（60s）未检测到消息被消费，退出码非零。失败诊断信息如下："
    log_error "--- 应用日志末尾 50 行 ---"
    tail -n 50 "$TEMP_DIR/app.log" || true
    log_error "--- 常见原因排查 ---"
    log_error "1. Redis 未运行或不在 localhost:6379（redis-cli ping 验证）"
    log_error "2. 端口 8080/6379 被占用"
    log_error "3. 残留的消费组位点数据（可更换 namespace 或 flushdb 后重试）"
    return 1
}

show_summary() {
    echo ""
    echo "╔══════════════════════════════════════════════════╗"
    echo "║          StreamMQ 快速开始演示完成！               ║"
    echo "╠══════════════════════════════════════════════════╣"
    echo "║                                                  ║"
    echo "║  演示应用已创建并启动                              ║"
    echo "║  位置: $TEMP_DIR"
    echo "║                                                  ║"
    echo "║  核心文件:                                       ║"
    echo "║    ├── pom.xml          Maven 配置               ║"
    echo "║    ├── application.yml  应用配置                 ║"
    echo "║    ├── DemoApplication.java  启动类              ║"
    echo "║    ├── DemoRunner.java       启动即发演示消息      ║"
    echo "║    ├── OrderService.java     消息发送             ║"
    echo "║    └── OrderConsumer.java    消息消费             ║"
    echo "║                                                  ║"
    echo "║  下一步:                                         ║"
    echo "║    1. 查看 $TEMP_DIR/app.log 观察发送/消费日志      ║"
    echo "║    2. 访问 http://localhost:8080/actuator/health  ║"
    echo "║    3. 尝试发送延时/事务/顺序消息                   ║"
    echo "║    4. 查看 streammq 管理端点                      ║"
    echo "║                                                  ║"
    echo "╚══════════════════════════════════════════════════╝"
    echo ""
}

cleanup() {
    log_step "清理..."
    if [ -n "${APP_PID:-}" ]; then
        kill "$APP_PID" 2>/dev/null || true
        log_info "已停止演示应用"
    fi
}

trap cleanup EXIT

main() {
    echo ""
    echo "╔══════════════════════════════════════════════════╗"
    echo "║     StreamMQ 快速开始演示                         ║"
    echo "║     从零开始：Redis → 依赖 → 配置 → 运行           ║"
    echo "╚══════════════════════════════════════════════════╝"
    echo ""

    check_prerequisites
    create_demo_app
    build_and_run
    wait_for_consumption
    show_summary
}

main "$@"