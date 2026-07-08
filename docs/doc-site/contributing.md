# 贡献指南

---

## 如何贡献

### 1. 提交 Issue

在 GitHub 提交 Issue 描述问题或建议：

- **Bug 报告**：描述问题、复现步骤、预期行为
- **功能请求**：描述期望的功能、使用场景
- **改进建议**：描述改进方向、收益

### 2. Fork 仓库

```bash
git fork https://github.com/streammq/streammq.git
```

### 3. 创建分支

```bash
git checkout -b feature/xxx
# 或
git checkout -b fix/xxx
```

### 4. 开发规范

#### 代码风格

遵循 [Google Java Style](https://google.github.io/styleguide/javaguide.html)。

#### 提交信息

遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```bash
git commit -m "feat: add xxx feature"
git commit -m "fix: resolve xxx bug"
git commit -m "docs: update xxx documentation"
git commit -m "refactor: optimize xxx code"
```

#### 编译检查

```bash
mvn -B -DskipTests compile
```

#### 依赖收敛

```bash
mvn enforcer:enforce
```

### 5. 测试要求

#### 单元测试

- 文件名以 `*Test.java` 命名
- 放在 `src/test/java` 目录

#### 集成测试

- 文件名以 `*IT.java` 命名
- 需要 Redis 7.2+ 实例
- 使用 Testcontainers 启动嵌入式 Redis

#### 测试运行

```bash
# 运行所有测试
mvn test

# 运行特定测试
mvn test -Dtest=MessageTest

# 运行集成测试
mvn test -Dtest=*IT
```

### 6. 提交 PR

发起 Pull Request 至 `main` 分支：

- 描述变更内容
- 关联相关 Issue
- 提供测试结果

### 7. Code Review

维护者审查通过后合并。

---

## 代码规范要点

### JDK 版本

- JDK 21+
- 可使用 record / sealed / pattern matching 等新特性

### 依赖管理

- 所有传递依赖必须版本收敛
- 使用 BOM 统一版本管理

### API 设计

- 公共 API 需添加 Javadoc
- 保持与 RocketMQ API 风格一致
- 使用 Builder 模式构建复杂对象

### 异常处理

- 使用自定义异常类
- 提供详细的错误信息
- 避免吞掉异常

### 日志规范

- 使用 SLF4J 接口
- 日志级别：ERROR > WARN > INFO > DEBUG > TRACE
- 关键路径必须有日志

---

## 模块开发

### 新建模块

```bash
mvn archetype:generate \
  -DgroupId=io.github.streammq \
  -DartifactId=streammq-new-module \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DinteractiveMode=false
```

### 模块命名

| 类型 | 命名规则 | 示例 |
|------|----------|------|
| 核心模块 | `streammq-core` | streammq-core |
| 适配器 | `streammq-*-adapter` | streammq-redisson-adapter |
| Starter | `streammq-*-starter` | streammq-spring-boot-starter |
| 兼容层 | `streammq-*-compat` | streammq-kafka-compat |
| 示例 | `streammq-sample-*` | streammq-sample-quickstart |

---

## 文档规范

### API 文档

- 使用 Javadoc 格式
- 描述方法功能、参数、返回值
- 提供代码示例

### 文档更新

- 功能变更时更新相关文档
- 保持文档与代码一致
- 使用 Markdown 格式

---

## 许可证

本项目基于 MIT 协议开源，贡献代码需遵守相同协议。