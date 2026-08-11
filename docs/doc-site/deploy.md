# 部署指南

> 本文档面向准备将 StreamMQ 部署到开发、测试、生产环境的工程师，覆盖从环境准备、单机部署到 Kubernetes 集群部署的完整流程，并给出生产环境的最佳实践配置。

---

## 目录

- [环境准备](#环境准备)
- [Redis 安装](#redis-安装)
- [Redis 生产配置](#redis-生产配置)
- [Maven 构建](#maven-构建)
- [Spring Boot 部署](#spring-boot-部署)
- [Docker 部署](#docker-部署)
- [Docker Compose 部署](#docker-compose-部署)
- [Kubernetes 部署](#kubernetes-部署)
- [生产环境最佳实践](#生产环境最佳实践)
- [健康检查端点](#健康检查端点)
- [扩缩容策略](#扩缩容策略)
- [优雅停机配置](#优雅停机配置)
- [安全配置](#安全配置)

---

## 环境准备

StreamMQ 基于 Redis Stream + Redisson 构建，部署前请确认以下软件版本要求。

### 软件版本要求

| 组件          | 最低版本 | 推荐版本   | 说明                                          |
| ------------- | -------- | ---------- | --------------------------------------------- |
| JDK           | 21       | 21         | StreamMQ 使用 record / sealed / 虚拟线程等特性 |
| Redis         | 7.2      | 7.2+       | 依赖 Redis Stream 的 `XAUTOCLAIM` 等新特性     |
| Maven         | 3.9      | 3.9+       | 构建工具，需支持 JDK 21                        |
| Spring Boot   | 3.3.0    | 3.3.5      | 与 StreamMQ Starter 自动装配匹配               |
| Docker        | 24.0     | 25.0+      | 容器化部署（可选）                             |
| Kubernetes    | 1.27     | 1.29+      | K8s 部署（可选）                               |

### JDK 21 安装与校验

推荐使用 Eclipse Temurin 或 Oracle JDK 21。

```bash
# Ubuntu/Debian (Eclipse Temurin)
sudo apt update
sudo apt install -y temurin-21-jdk

# CentOS/RHEL (Eclipse Temurin)
sudo yum install -y temurin-21-jdk

# macOS (Homebrew)
brew install --cask temurin@21

# 校验版本
java -version
```

预期输出：

```
openjdk version "21" 2023-09-19
OpenJDK Runtime Environment (build 21+35-2513)
OpenJDK 64-Bit Server VM (build 21+35-2513, mixed mode, sharing)
```

### Maven 安装与校验

```bash
# Ubuntu/Debian
sudo apt install -y maven

# macOS
brew install maven

# 校验版本
mvn -v
```

预期输出（版本需 ≥ 3.9）：

```
Apache Maven 3.9.6 (bc0240f3c744dd6b6ec2920b3cd08dcc295161ae)
Maven home: /usr/share/maven
Java version: 21, vendor: Eclipse Adoptium
```

---

## Redis 安装

StreamMQ 仅依赖 Redis 7.2+，无需引入独立 MQ 集群。下面给出常见安装方式。

### Ubuntu/Debian 安装

```bash
sudo apt update
sudo apt install -y redis-server

# 启动并设置开机自启
sudo systemctl enable --now redis-server

# 校验
redis-server --version
redis-cli ping   # 返回 PONG 即可
```

### CentOS/RHEL 安装

```bash
sudo yum install -y redis
sudo systemctl enable --now redis

redis-server --version
redis-cli ping
```

### Docker 快速安装

```bash
docker run -d \
  --name redis \
  -p 6379:6379 \
  -v redis-data:/data \
  redis:7.2-alpine \
  redis-server --appendonly yes --maxmemory 512mb --maxmemory-policy noeviction
```

### 源码编译安装（生产推荐）

```bash
wget https://download.redis.io/releases/redis-7.2.4.tar.gz
tar xzf redis-7.2.4.tar.gz
cd redis-7.2.4
make -j$(nproc)
sudo make install

# 启动
redis-server /etc/redis/redis.conf
```

---

## Redis 生产配置

生产环境推荐的 `redis.conf` 关键配置项：

```conf
# ========== 网络 ==========
bind 0.0.0.0
protected-mode yes
port 6379
tcp-backlog 511
tcp-keepalive 300
timeout 0

# ========== 内存 ==========
maxmemory 4gb
maxmemory-policy noeviction

# ========== 持久化：AOF ==========
appendonly yes
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# ========== 持久化：RDB（备份用） ==========
save 900 1
save 300 10
save 60 10000
dbfilename dump.rdb
dir /var/lib/redis

# ========== Stream 相关 ==========
# Stream 的最大长度建议由 StreamMQ 在创建时指定，避免无限增长
# 通过 streammq 配置 max-stream-length 控制

# ========== 慢查询 ==========
slowlog-log-slower-than 10000
slowlog-max-len 128

# ========== 客户端缓冲区 ==========
client-output-buffer-limit stream 256mb 64mb 60
client-output-buffer-limit replica 256mb 64mb 60
client-output-buffer-limit pubsub 32mb 8mb 60

# ========== 安全 ==========
requirepass YourStrongPasswordHere
rename-command FLUSHDB ""
rename-command FLUSHALL ""
rename-command KEYS ""
```

> **注意：** StreamMQ 不强制要求 `maxmemory-policy noeviction`，但强烈推荐使用 `noeviction` 而非 `allkeys-lru`，以避免 Stream 数据被意外驱逐导致消息丢失。

---

## Maven 构建

### 克隆项目

```bash
git clone https://github.com/streammq/streammq.git
cd streammq
```

### 编译项目

```bash
# 完整编译（跳过测试）
mvn clean package -DskipTests

# 仅编译，不打包
mvn clean compile
```

### 运行测试

StreamMQ 当前包含 651 个测试用例，部分集成测试依赖 Testcontainers 启动嵌入式 Redis，需 Docker 环境。

```bash
# 运行所有测试（需要 Docker）
mvn clean test

# 运行集成测试
mvn clean verify

# 仅运行指定模块测试
mvn test -pl streammq-core
mvn test -pl streammq-redisson

# 运行单个测试类
mvn test -Dtest=MessageTest
```

### 生成测试覆盖率报告

```bash
mvn clean verify
# 报告位于各模块 target/site/jacoco/index.html
```

### 构建产物

构建完成后，关键产物位于：

| 模块                              | 产物                                | 用途                  |
| --------------------------------- | ----------------------------------- | --------------------- |
| `streammq-bom`                    | `streammq-bom-0.1.0.pom`   | 版本管理 BOM          |
| `streammq-core`                   | `streammq-core-0.1.0.jar`  | 核心库                |
| `streammq-redisson`               | `streammq-redisson-0.1.0.jar` | Redisson 适配器     |
| `streammq-spring-boot-starter`    | `streammq-spring-boot-starter-0.1.0.jar` | Spring Boot Starter |
| `streammq-test`                   | `streammq-test-0.1.0.jar`  | 测试工具              |

---

## Spring Boot 部署

### 打包应用

在你的 Spring Boot 项目中执行：

```bash
mvn clean package -DskipTests
# 产物：target/your-app.jar
```

### 运行应用

```bash
# 前台运行
java -jar your-app.jar

# 后台运行
nohup java -jar your-app.jar > app.log 2>&1 &
```

### 指定配置文件位置

```bash
# 外部化配置
java -jar your-app.jar --spring.config.location=file:/etc/streammq/application.yml

# 额外配置（覆盖 jar 包内默认值）
java -jar your-app.jar --spring.config.additional-location=file:/etc/streammq/

# 通过环境变量覆盖
SPRING_PROFILES_ACTIVE=prod java -jar your-app.jar
```

### 推荐的最小化生产配置

```yaml
spring:
  application:
    name: streammq-app
  profiles:
    active: prod

streammq:
  enabled: true
  namespace: streammq
  producer:
    group: default-producer-group
    send-timeout: 3000
    retry-times: 2
  consumer:
    consume-thread-min: 4
    consume-thread-max: 64
    pull-batch-size: 32
    max-reconsume-times: 16
    consume-timeout: 30000
    inflight-capacity: 1000
  transaction:
    check-interval-ms: 60000
    max-check-times: 15
  dlq:
    enabled: true
  metrics:
    enabled: true
  management:
    enabled: true

redisson:
  singleServerConfig:
    address: "redis://redis-host:6379"
    database: 0
    password: "${REDIS_PASSWORD}"
    connectionPoolSize: 64
    connectionMinimumIdleSize: 24
    idleConnectionTimeout: 10000
    connectTimeout: 10000
    timeout: 3000
    retryAttempts: 3
    retryInterval: 1500

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics,streammq
  endpoint:
    health:
      show-details: when-authorized
  metrics:
    tags:
      application: ${spring.application.name}
```

---

## Docker 部署

### Dockerfile

推荐的多阶段构建 Dockerfile：

```dockerfile
# ========== 阶段 1：构建 ==========
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# ========== 阶段 2：运行 ==========
FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="StreamMQ Contributors"
LABEL org.opencontainers.image.source="https://github.com/streammq/streammq"

# 安装 curl 用于健康检查
RUN apk add --no-cache curl tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

WORKDIR /app

# 创建非 root 用户
RUN addgroup -S streammq && adduser -S streammq -G streammq

# 复制构建产物
COPY --from=builder /build/target/*.jar app.jar

# 创建日志与 heapdump 目录
RUN mkdir -p /app/logs /app/heapdump && chown -R streammq:streammq /app

USER streammq

EXPOSE 8080

# JVM 参数：容器感知 + G1GC + Heap Dump
ENV JAVA_OPTS="-XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/app/heapdump \
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
```

### 构建镜像

```bash
docker build -t streammq-app:0.1.0 .
docker tag streammq-app:0.1.0 streammq-app:latest
```

### 运行容器

```bash
docker run -d \
  --name streammq-app \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=prod \
  -e REDIS_HOST=redis-host \
  -e REDIS_PORT=6379 \
  -e REDIS_PASSWORD=YourPassword \
  -v /etc/streammq/application.yml:/app/application.yml:ro \
  -v /var/log/streammq:/app/logs \
  --memory="1g" \
  --memory-swap="1g" \
  --cpus="1.0" \
  --restart=unless-stopped \
  streammq-app:0.1.0
```

### 容器健康检查

```bash
# 在 Dockerfile 中配置健康检查
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1
```

---

## Docker Compose 部署

适合开发、测试或小规模生产环境的一体化部署。

### docker-compose.yml

```yaml
version: '3.8'

services:
  redis:
    image: redis:7.2-alpine
    container_name: streammq-redis
    restart: unless-stopped
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
      - ./redis/redis.conf:/etc/redis/redis.conf:ro
    command: redis-server /etc/redis/redis.conf
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - streammq-net

  streammq-app:
    build: .
    container_name: streammq-app
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - SPRING_PROFILES_ACTIVE=prod
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - REDIS_PASSWORD=YourPassword
      - JAVA_OPTS=-XX:+UseG1GC -XX:MaxRAMPercentage=75.0 -XX:+HeapDumpOnOutOfMemoryError
    volumes:
      - ./config/application.yml:/app/application.yml:ro
      - app-logs:/app/logs
      - app-heapdump:/app/heapdump
    depends_on:
      redis:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/actuator/health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 60s
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '1.0'
        reservations:
          memory: 512M
          cpus: '0.5'
    networks:
      - streammq-net

  prometheus:
    image: prom/prometheus:latest
    container_name: streammq-prometheus
    restart: unless-stopped
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    depends_on:
      - streammq-app
    networks:
      - streammq-net

  grafana:
    image: grafana/grafana:latest
    container_name: streammq-grafana
    restart: unless-stopped
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    depends_on:
      - prometheus
    networks:
      - streammq-net

volumes:
  redis-data:
  app-logs:
  app-heapdump:
  prometheus-data:
  grafana-data:

networks:
  streammq-net:
    driver: bridge
```

### 启动

```bash
# 启动全部服务
docker compose up -d

# 查看日志
docker compose logs -f streammq-app

# 扩容应用（多副本需配合集群消费）
docker compose up -d --scale streammq-app=3
```

---

## Kubernetes 部署

### Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: streammq
  labels:
    app.kubernetes.io/part-of: streammq
```

### ConfigMap - 应用配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: streammq-app-config
  namespace: streammq
data:
  application.yml: |
    spring:
      application:
        name: streammq-app
    streammq:
      enabled: true
      namespace: streammq
      producer:
        group: default-producer-group
        send-timeout: 3000
        retry-times: 2
      consumer:
        consume-thread-min: 4
        consume-thread-max: 64
        pull-batch-size: 32
        max-reconsume-times: 16
        consume-timeout: 30000
        inflight-capacity: 1000
      transaction:
        check-interval-ms: 60000
        max-check-times: 15
      dlq:
        enabled: true
      metrics:
        enabled: true
      management:
        enabled: true
    management:
      endpoints:
        web:
          exposure:
            include: health,info,prometheus,metrics,streammq
```

### ConfigMap - Redis 配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: redis-config
  namespace: streammq
data:
  redis.conf: |
    bind 0.0.0.0
    protected-mode yes
    port 6379
    maxmemory 2gb
    maxmemory-policy noeviction
    appendonly yes
    appendfsync everysec
    save 900 1
    save 300 10
    save 60 10000
```

### Secret - 敏感信息

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: streammq-secret
  namespace: streammq
type: Opaque
stringData:
  redis-password: YourStrongPasswordHere
```

### Redis StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
  namespace: streammq
spec:
  serviceName: redis
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7.2-alpine
          ports:
            - containerPort: 6379
          env:
            - name: REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: streammq-secret
                  key: redis-password
          command: ["redis-server"]
          args: ["/etc/redis/redis.conf", "--requirepass", "$(REDIS_PASSWORD)"]
          volumeMounts:
            - name: redis-config
              mountPath: /etc/redis/redis.conf
              subPath: redis.conf
            - name: redis-data
              mountPath: /data
          resources:
            limits:
              memory: "2Gi"
              cpu: "1000m"
            requests:
              memory: "1Gi"
              cpu: "500m"
          livenessProbe:
            exec:
              command: ["redis-cli", "ping"]
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            exec:
              command: ["redis-cli", "ping"]
            initialDelaySeconds: 5
            periodSeconds: 5
      volumes:
        - name: redis-config
          configMap:
            name: redis-config
  volumeClaimTemplates:
    - metadata:
        name: redis-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: streammq
spec:
  selector:
    app: redis
  ports:
    - port: 6379
      targetPort: 6379
  clusterIP: None
```

### 应用 Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streammq-app
  namespace: streammq
spec:
  replicas: 3
  selector:
    matchLabels:
      app: streammq-app
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    metadata:
      labels:
        app: streammq-app
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/actuator/prometheus"
    spec:
      terminationGracePeriodSeconds: 60
      containers:
        - name: streammq-app
          image: streammq-app:0.1.0
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: SPRING_PROFILES_ACTIVE
              value: "prod"
            - name: REDIS_HOST
              value: "redis.streammq.svc.cluster.local"
            - name: REDIS_PORT
              value: "6379"
            - name: REDIS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: streammq-secret
                  key: redis-password
            - name: JAVA_OPTS
              value: >-
                -XX:+UseG1GC
                -XX:MaxGCPauseMillis=200
                -XX:+HeapDumpOnOutOfMemoryError
                -XX:HeapDumpPath=/app/heapdump
                -XX:+UseContainerSupport
                -XX:MaxRAMPercentage=75.0
          volumeMounts:
            - name: app-config
              mountPath: /app/application.yml
              subPath: application.yml
            - name: heapdump
              mountPath: /app/heapdump
          resources:
            limits:
              memory: "1Gi"
              cpu: "1000m"
            requests:
              memory: "512Mi"
              cpu: "500m"
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8080
            initialDelaySeconds: 60
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 5
            failureThreshold: 3
          lifecycle:
            preStop:
              exec:
                command: ["sh", "-c", "sleep 10"]
      volumes:
        - name: app-config
          configMap:
            name: streammq-app-config
        - name: heapdump
          emptyDir: {}
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: streammq-app
  namespace: streammq
spec:
  type: ClusterIP
  selector:
    app: streammq-app
  ports:
    - name: http
      port: 80
      targetPort: 8080
      protocol: TCP
```

### HPA 水平自动扩缩容

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: streammq-app-hpa
  namespace: streammq
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: streammq-app
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
    - type: Pods
      pods:
        metric:
          name: streammq_consumer_lag
        target:
          type: AverageValue
          averageValue: "100"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
        - type: Percent
          value: 100
          periodSeconds: 60
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Percent
          value: 50
          periodSeconds: 60
```

### 部署命令

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap-app.yaml
kubectl apply -f configmap-redis.yaml
kubectl apply -f secret.yaml
kubectl apply -f redis-statefulset.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f hpa.yaml

# 查看部署状态
kubectl -n streammq get pods
kubectl -n streammq logs -f deployment/streammq-app
```

---

## 生产环境最佳实践

### Redis 配置最佳实践

| 配置项                  | 推荐值             | 说明                                                |
| ----------------------- | ------------------ | --------------------------------------------------- |
| `maxmemory`             | 物理内存的 60-70%  | 给操作系统和 RDB/AOF fork 留出空间                  |
| `maxmemory-policy`      | `noeviction`       | **必须**，避免 Stream 数据被 LRU 驱逐导致丢消息     |
| `appendonly`            | `yes`              | 开启 AOF 持久化，确保消息不丢                       |
| `appendfsync`           | `everysec`         | 平衡性能与可靠性，最多丢失 1 秒数据                 |
| `client-output-buffer-limit stream` | `256mb 64mb 60` | 防止消费者慢导致 Redis 内存溢出                     |
| `tcp-keepalive`         | `300`              | 防止僵尸连接                                        |
| `repl-backlog-size`     | `64mb`+            | 主从复制场景下防止全量同步                          |

### JVM 配置最佳实践

```bash
java \
  # ===== GC 配置 =====
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:G1HeapRegionSize=16m \
  -XX:InitiatingHeapOccupancyPercent=45 \
  \
  # ===== 容器感知 =====
  -XX:+UseContainerSupport \
  -XX:MaxRAMPercentage=75.0 \
  -XX:InitialRAMPercentage=50.0 \
  \
  # ===== Heap Dump =====
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/app/heapdump/ \
  -XX:OnOutOfMemoryError="kill -9 %p" \
  \
  # ===== GC 日志 =====
  -Xlog:gc*:file=/app/logs/gc.log:time,uptime,level,tags:filecount=10,filesize=100m \
  \
  # ===== 其他 =====
  -Djava.security.egd=file:/dev/./urandom \
  -Dfile.encoding=UTF-8 \
  -Duser.timezone=Asia/Shanghai \
  -jar your-app.jar
```

| 参数                              | 推荐值              | 说明                                     |
| --------------------------------- | ------------------- | ---------------------------------------- |
| `-XX:+UseG1GC`                    | -                   | JDK 21 推荐使用 G1GC                     |
| `-XX:MaxRAMPercentage`            | `75.0`              | 容器内堆内存占比，留 25% 给堆外和元空间  |
| `-XX:+UseContainerSupport`        | -                   | 启用容器感知（JDK 10+ 默认开启）         |
| `-XX:+HeapDumpOnOutOfMemoryError` | -                   | OOM 时自动 dump                          |
| `-XX:MaxGCPauseMillis`            | `200`               | 目标最大 GC 停顿时间                     |

### 监控配置最佳实践

#### Actuator + Prometheus + Grafana

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics,streammq
      base-path: /actuator
  endpoint:
    health:
      show-details: when-authorized
      probes:
        enabled: true   # 启用 liveness/readiness 探针
  health:
    livenessstate:
      enabled: true
    readinessstate:
      enabled: true
  metrics:
    enabled: true
    tags:
      application: ${spring.application.name}
      namespace: ${streammq.namespace}
    distribution:
      percentiles-histogram:
        streammq.producer.send.duration: true
        streammq.consumer.consume.duration: true
      percentiles:
        streammq.producer.send.duration: 0.5,0.9,0.99
        streammq.consumer.consume.duration: 0.5,0.9,0.99
```

#### Prometheus 抓取配置

```yaml
scrape_configs:
  - job_name: 'streammq-app'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 15s
    static_configs:
      - targets: ['streammq-app:8080']
        labels:
          app: 'streammq'
```

#### 关键告警规则（Prometheus）

```yaml
groups:
  - name: streammq
    rules:
      - alert: StreamMQProducerErrorRateHigh
        expr: |
          rate(streammq_producer_send_failed_total[5m])
          / rate(streammq_producer_send_total[5m]) > 0.05
        for: 5m
        annotations:
          summary: "StreamMQ 生产者错误率 > 5%"
      - alert: StreamMQConsumerLagHigh
        expr: streammq_consumer_lag > 1000
        for: 10m
        annotations:
          summary: "StreamMQ 消费堆积超过 1000"
      - alert: StreamMQDlqGrowing
        expr: rate(streammq_consumer_dlq_total[10m]) > 0
        for: 10m
        annotations:
          summary: "StreamMQ 死信队列持续增长"
```

### 连接池调优

Redisson 连接池参数需要根据并发量和 Redis 实例规格调整：

```yaml
redisson:
  singleServerConfig:
    connectionPoolSize: 64           # 连接池大小（默认 64）
    connectionMinimumIdleSize: 24    # 最小空闲连接（默认 24）
    idleConnectionTimeout: 10000     # 空闲连接超时（ms）
    connectTimeout: 10000            # 建连超时（ms）
    timeout: 3000                    # 命令超时（ms）
    retryAttempts: 3                 # 重试次数
    retryInterval: 1500              # 重试间隔（ms）
    pingConnectionInterval: 30000    # 心跳间隔（ms）
    keepAlive: true
    tcpNoDelay: true
```

| 场景                  | connectionPoolSize | connectionMinimumIdleSize | 备注                       |
| --------------------- | ------------------ | ------------------------- | -------------------------- |
| 开发/测试             | 16                 | 8                         | 资源有限                   |
| 中等并发（1k TPS）    | 64                 | 24                        | 默认值                     |
| 高并发（10k TPS）     | 128                | 32                        | 需对应调整 Redis `maxclients` |
| 顺序消费场景          | shardCount × 2     | shardCount                | 每分片需独立连接           |

### 消费线程调优

```yaml
streammq:
  consumer:
    consume-thread-min: 4        # 最小线程数
    consume-thread-max: 64       # 最大线程数
    pull-batch-size: 32          # 单次拉取批量
    inflight-capacity: 1000      # 背压队列容量
    consume-timeout: 30000       # 消费超时（ms）
```

调优建议：

| 业务类型            | consumeThreadMax | pullBatchSize | inflightCapacity | 备注                       |
| ------------------- | ---------------- | -------------| ----------------- | -------------------------- |
| CPU 密集型          | CPU 核数 × 2     | 16            | 500               | 避免线程过多导致上下文切换 |
| IO 密集型（DB/RPC） | CPU 核数 × 10    | 32            | 1000              | 充分利用 IO 等待时间       |
| 混合型              | CPU 核数 × 4     | 32            | 1000              | 平衡                       |
| 顺序消费            | shardCount       | 8             | shardCount × 2    | 单分片串行                 |

---

## 健康检查端点

StreamMQ 通过 Spring Boot Actuator 暴露以下端点：

| 端点                          | 方法   | 说明                                  | 是否敏感 |
| ----------------------------- | ------ | ------------------------------------- | -------- |
| `/actuator/health`            | GET    | 健康检查（含 Redis、StreamMQ 状态）   | 是       |
| `/actuator/health/liveness`   | GET    | Liveness 探针（K8s）                  | 否       |
| `/actuator/health/readiness`  | GET    | Readiness 探针（K8s）                 | 否       |
| `/actuator/info`              | GET    | 应用元信息                            | 否       |
| `/actuator/metrics`           | GET    | 指标列表                              | 是       |
| `/actuator/metrics/{name}`    | GET    | 单个指标详情                          | 是       |
| `/actuator/prometheus`        | GET    | Prometheus 格式指标                   | 是       |
| `/actuator/streammq`          | GET    | StreamMQ 管理端点                     | 是       |

### 健康检查示例

```bash
curl http://localhost:8080/actuator/health
```

预期输出：

```json
{
  "status": "UP",
  "components": {
    "redis": {
      "status": "UP",
      "details": {
        "version": "7.2.4"
      }
    },
    "streammq": {
      "status": "UP",
      "details": {
        "namespace": "streammq",
        "consumerGroups": 3,
        "producers": 1
      }
    },
    "diskSpace": {
      "status": "UP",
      "details": {
        "total": 53660876800,
        "free": 31861760000,
        "threshold": 10485760
      }
    }
  }
}
```

### Liveness / Readiness 探针（K8s）

```bash
# Liveness：应用是否存活
curl http://localhost:8080/actuator/health/liveness
# {"status":"UP"}

# Readiness：应用是否准备好接收流量
curl http://localhost:8080/actuator/health/readiness
# {"status":"UP"}
```

---

## 扩缩容策略

### 水平扩缩容

StreamMQ 的消费者天然支持水平扩展，新增消费者实例会自动加入消费组并参与分摊。

#### 集群消费模式

```
扩容前：3 个实例，每实例处理 1/3 消息
扩容后：6 个实例，每实例处理 1/6 消息
```

**操作步骤：**

```bash
# Kubernetes 扩容
kubectl -n streammq scale deployment streammq-app --replicas=6

# 或通过 HPA 自动扩容（参考上文 HPA 配置）
```

#### 广播消费模式

广播模式下每个实例都会消费全量消息，扩容不影响每实例的处理量，仅增加冗余。

### 分片数（shardCount）规划

顺序消费场景下，分片数决定了并行度上限：

| 业务规模          | 推荐 shardCount | 备注                            |
| ----------------- | --------------- | ------------------------------- |
| 小规模（< 1k TPS）| 4               | 默认值                          |
| 中规模（1k-5k TPS）| 8-16           | 需对应增加消费线程              |
| 大规模（> 5k TPS）| 32-64           | 需评估 Redis 连接数与负载       |

> **注意：** `shardCount` 一旦确定后变更需谨慎，已存在的消息会按原分片路由。建议在创建 Topic 时即规划好分片数。

### 消费者实例数规划

| 消费模式 | 推荐实例数                     | 超过分片数后的行为                  |
| -------- | ------------------------------ | ----------------------------------- |
| 集群消费 | 不限，由流量决定               | 多实例分摊消息                      |
| 顺序消费 | ≤ shardCount                   | 超过 shardCount 的实例会空闲        |
| 广播消费 | 不限，每实例消费全量           | 仅增加冗余，不分摊                  |

---

## 优雅停机配置

### Spring Boot 配置

```yaml
server:
  shutdown: graceful            # 启用优雅停机

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s   # 优雅停机最大等待时间

streammq:
  consumer:
    shutdown-timeout: 25000     # 消费者停止前等待正在处理消息完成的最长时间（ms）
```

### K8s 配合

```yaml
spec:
  terminationGracePeriodSeconds: 60    # K8s 给应用的最大终止宽限期
  containers:
    - name: streammq-app
      lifecycle:
        preStop:
          exec:
            command: ["sh", "-c", "sleep 10"]   # 给 LB 摘除流量留时间
```

### 优雅停机流程

```
1. K8s 发送 SIGTERM 信号
2. preStop hook 执行 sleep 10（等待 LB 摘除流量）
3. Spring Boot 接收信号，停止接收新请求
4. StreamMQ 消费者停止拉取新消息
5. 等待正在处理的消息完成（最长 25 秒）
6. 提交消费进度（XACK）
7. 关闭 Redisson 连接
8. 应用退出
```

---

## 安全配置

### ManagementAuthenticator

StreamMQ 提供管理端点的 SPI 鉴权扩展点 `ManagementAuthenticator`，默认实现为 `AllowAllAuthenticator`（放行所有请求）。生产环境必须替换为自定义实现：

```java
public class BasicAuthAuthenticator implements ManagementAuthenticator {
    @Override
    public boolean authenticate(HttpServletRequest request) {
        String auth = request.getHeader("Authorization");
        if (StringUtils.isEmpty(auth) || !auth.startsWith("Basic ")) {
            return false;
        }
        String decoded = new String(Base64.getDecoder().decode(auth.substring(6)));
        String[] parts = decoded.split(":", 2);
        return verifyCredentials(parts[0], parts[1]);
    }
}
```

通过 SPI 注册（在 `META-INF/services/io.github.streammq.core.policy.ManagementAuthenticator` 文件中写入实现类全名）。

### Redis ACL 配置

生产环境推荐使用 Redis ACL 为 StreamMQ 创建专用用户：

```conf
# redis.conf
user streammq on >YourStrongPassword ~streammq:* +@all -@dangerous
```

含义：
- 创建用户 `streammq`，密码为 `YourStrongPassword`
- 仅允许访问 `streammq:*` 前缀的 key（与 `streammq.namespace` 对应）
- 允许所有非危险命令

### Redis TLS 配置

```yaml
redisson:
  singleServerConfig:
    address: "rediss://redis-host:6379"    # 注意是 rediss://
    sslProvider: JDK
    sslTruststore: "classpath:redis-truststore.jks"
    sslTruststorePassword: "${SSL_TRUSTSTORE_PASSWORD}"
    sslKeystore: "classpath:redis-keystore.jks"
    sslKeystorePassword: "${SSL_KEYSTORE_PASSWORD}"
```

### 应用层安全配置

```yaml
spring:
  security:
    user:
      name: admin
      password: "${ADMIN_PASSWORD}"
      roles: ACTUATOR_ADMIN

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  endpoint:
    health:
      show-details: when-authorized
```

### 网络安全

| 措施                 | 实施方式                                                |
| -------------------- | ------------------------------------------------------- |
| Redis 仅内网访问     | 不暴露公网 IP，使用 VPC 内网地址                         |
| 安全组限制           | Redis 端口仅允许应用服务器访问                          |
| 应用层防火墙         | WAF + 反向代理（Nginx）                                 |
| mTLS                 | 服务网格（Istio/Linkerd）启用双向 TLS                   |

---

## 故障排查清单

| 现象                | 排查步骤                                                            |
| ------------------- | ------------------------------------------------------------------- |
| 应用启动失败        | 1. 检查 Redis 连通性 2. 检查 namespace 权限 3. 查看启动日志         |
| 消息发送超时        | 1. 检查 Redis 负载 2. 调整 `send-timeout` 3. 增大连接池             |
| 消费者不消费        | 1. 检查消费者组是否创建 2. 检查消费进度 3. 检查 `selectorExpression`|
| OOM                 | 1. 调小 `inflight-capacity` 2. 调小 `pull-batch-size` 3. 检查内存泄漏 |
| Redis 内存告警      | 1. 检查 Stream 长度 2. 调整 `maxmemory` 3. 启用 Stream trim        |

---

## 参考资源

- [StreamMQ GitHub](https://github.com/streammq/streammq)
- [Redis 7.2 文档](https://redis.io/docs/7.2/)
- [Redisson 配置参考](https://github.com/redisson/redisson/wiki/2.-Configuration)
- [Spring Boot 3.3 Actuator](https://docs.spring.io/spring-boot/docs/3.3.x/reference/html/actuator.html)
- [Kubernetes 最佳实践](https://kubernetes.io/docs/setup/best-practices/)

---

*StreamMQ · 让 Redis 成为你的消息总线。*
