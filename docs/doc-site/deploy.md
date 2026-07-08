# 部署指南

---

## 环境准备

### JDK 安装

StreamMQ 需要 JDK 21+：

```bash
# 检查 Java 版本
java -version

# 输出示例
openjdk version "21" 2023-09-19
OpenJDK Runtime Environment (build 21+35-2513)
OpenJDK 64-Bit Server VM (build 21+35-2513, mixed mode, sharing)
```

### Redis 安装

StreamMQ 需要 Redis 7.2+：

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install redis-server

# 检查 Redis 版本
redis-server --version

# 启动 Redis
redis-server /etc/redis/redis.conf
```

---

## Maven 构建

### 克隆项目

```bash
git clone https://github.com/streammq/streammq.git
cd streammq
```

### 编译项目

```bash
mvn clean package -DskipTests
```

### 运行测试

```bash
# 需要 Docker 环境运行嵌入式 Redis
mvn test
```

---

## Spring Boot 部署

### 打包应用

```bash
# 在你的项目中打包
mvn clean package -DskipTests
```

### 运行应用

```bash
java -jar your-app.jar
```

### 指定配置文件

```bash
java -jar your-app.jar --spring.config.location=file:/path/to/application.yml
```

---

## Docker 部署

### 编写 Dockerfile

```dockerfile
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY target/your-app.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
```

### 构建镜像

```bash
docker build -t streammq-app .
```

### 运行容器

```bash
docker run -d \
  --name streammq-app \
  -p 8080:8080 \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  streammq-app
```

### Docker Compose

```yaml
version: '3.8'

services:
  redis:
    image: redis:7.2-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

  streammq-app:
    build: .
    ports:
      - "8080:8080"
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
    depends_on:
      - redis

volumes:
  redis-data:
```

---

## Kubernetes 部署

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streammq-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: streammq-app
  template:
    metadata:
      labels:
        app: streammq-app
    spec:
      containers:
        - name: streammq-app
          image: streammq-app:latest
          ports:
            - containerPort: 8080
          env:
            - name: REDIS_HOST
              value: "redis-master"
            - name: REDIS_PORT
              value: "6379"
          resources:
            limits:
              memory: "512Mi"
              cpu: "500m"
            requests:
              memory: "256Mi"
              cpu: "250m"
```

### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: streammq-app
spec:
  type: ClusterIP
  selector:
    app: streammq-app
  ports:
    - port: 80
      targetPort: 8080
```

### Redis 配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: redis-config
data:
  redis.conf: |
    maxmemory-policy allkeys-lru
    maxmemory 512mb
```

---

## 生产环境建议

### Redis 配置

```yaml
# redis.conf
maxmemory-policy allkeys-lru
maxmemory 4gb
appendonly yes
appendfsync everysec
```

### JVM 配置

```bash
java -jar your-app.jar \
  -Xms512m \
  -Xmx1g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/tmp/heapdump.hprof
```

### 监控配置

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  metrics:
    tags:
      application: ${spring.application.name}
```

---

## 健康检查

### Actuator 端点

| 端点 | 说明 |
|------|------|
| `/actuator/health` | 健康检查 |
| `/actuator/info` | 应用信息 |
| `/actuator/metrics` | 指标信息 |
| `/actuator/prometheus` | Prometheus 格式指标 |

### 健康检查示例

```bash
curl http://localhost:8080/actuator/health

# 输出
{
  "status": "UP",
  "components": {
    "redis": { "status": "UP" },
    "streammq": { "status": "UP" }
  }
}
```