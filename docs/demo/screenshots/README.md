# StreamMQ 发布截图清单

本目录用于存放 StreamMQ 发布时所需的截图素材。

## 截图清单

### 1. 项目主页截图（GitHub / Gitee）

| 文件名 | 描述 | 建议尺寸 |
|--------|------|----------|
| `home-hero.png` | GitHub 仓库首页 Hero 区域，展示项目徽章、标题、简介 | 1200×630 |
| `architecture-overview.png` | README 中的架构总览图（ASCII 架构图的美化版本） | 1600×900 |
| `feature-comparison.png` | 与同类产品对比表格的截图 | 1400×1000 |

### 2. 快速开始截图

| 文件名 | 描述 | 建议尺寸 |
|--------|------|----------|
| `quickstart-dependencies.png` | Maven 依赖配置代码高亮截图 | 1200×400 |
| `quickstart-config.png` | application.yml 配置截图 | 1200×400 |
| `quickstart-enable.png` | @EnableStreamMQ 注解代码截图 | 800×300 |
| `quickstart-send.png` | 消息发送代码截图 | 1000×400 |
| `quickstart-consume.png` | 消息消费者代码截图 | 1000×400 |
| `quickstart-run.png` | 应用启动成功、消息发送/消费的控制台输出 | 1200×600 |

### 3. 核心特性截图

| 文件名 | 描述 | 建议尺寸 |
|--------|------|----------|
| `feature-transaction.png` | 事务消息代码示例 + 运行效果 | 1400×600 |
| `feature-delay.png` | 延时消息代码示例（固定延时 + 任意延时） | 1400×600 |
| `feature-orderly.png` | 顺序消息代码示例 + 消费日志 | 1400×600 |
| `feature-dlq.png` | 死信队列处理截图 | 1200×500 |
| `feature-filter.png` | Tag/SQL92 消息过滤截图 | 1200×500 |
| `feature-compression.png` | 消息压缩配置截图 | 1000×400 |

### 4. 可观测性截图

| 文件名 | 描述 | 建议尺寸 |
|--------|------|----------|
| `actuator-health.png` | `/actuator/health` 端点响应 | 1200×600 |
| `actuator-metrics.png` | `/actuator/metrics` 端点响应 | 1200×800 |
| `management-api.png` | 管理 REST API 调用截图（消费组/Topic/DLQ 查询） | 1400×800 |
| `dashboard-grafana.png` | Grafana 监控面板截图（如有） | 1600×900 |

### 5. Demo 应用截图

| 文件名 | 描述 | 建议尺寸 |
|--------|------|----------|
| `demo-app-list.png` | streammq-samples 示例工程列表 | 1200×500 |
| `demo-quickstart-run.png` | quickstart 示例运行截图 | 1400×700 |
| `demo-transaction-run.png` | transaction 示例运行截图 | 1400×700 |
| `demo-delay-run.png` | delay 示例运行截图 | 1400×700 |

### 6. 发布用社交素材

| 文件名 | 描述 | 建议尺寸 |
|--------|------|----------|
| `og-image.png` | Open Graph 分享卡片（用于 Twitter/LinkedIn） | 1200×630 |
| `social-hero-1.png` | 社交媒体横幅 1 - "零额外部署" | 1200×675 |
| `social-hero-2.png` | 社交媒体横幅 2 - "类 RocketMQ API" | 1200×675 |
| `social-hero-3.png` | 社交媒体横幅 3 - "12 个 SPI 扩展点" | 1200×675 |

## 截图规范

- **背景**：浅色/深色主题各一套
- **字体**：代码使用等宽字体（JetBrains Mono / Cascadia Code）
- **浏览器窗口**：使用 Chrome 开发者工具，100% 缩放
- **代码高亮**：使用 Prism.js 或 Shiki 统一主题
- **命名规范**：`{category}-{description}.png`，全小写，连字符分隔

## 推荐工具

- **截图**：Snipaste / CleanShot X
- **代码美化**：Carbon (https://carbon.now.sh)
- **GIF 录制**：LICEcap / ScreenToGif / Kap
- **图片压缩**：TinyPNG / Squoosh

## 相关文件

- [演示 GIF 制作指南](../demo-gif-guide.md)
- [快速开始演示脚本](../quickstart-demo.sh)