# StreamMQ 演示 GIF 制作指南

本指南用于录制 StreamMQ 的演示 GIF，用于 YouTube、Product Hunt、GitHub 展示。

---

## 1. 设备与环境准备

### 1.1 推荐设备

- **显示器**：2K 或 4K 分辨率
- **浏览器**：Chrome / Edge 最新版
- **代码编辑器**：VS Code（Dark+ 主题，JetBrains Mono 字体 14px）
- **操作系统**：macOS / Windows / Linux（保持一致性）

### 1.2 推荐工具

| 工具 | 用途 | 许可 |
|------|------|------|
| [Kap](https://getkap.co) | macOS GIF 录制（免费开源） | MIT |
| [ScreenToGif](https://www.screentogif.com) | Windows GIF 录制（免费） | Free |
| [LICEcap](https://www.cockos.com/licecap/) | 跨平台 GIF 录制 | Free |
| [CloudConvert](https://cloudconvert.com) | GIF 压缩/转换 | Web |
| [Gifski](https://gif.ski) | GIF 高质量压缩 | Free |

### 1.3 录制前准备

```bash
# 1. 确保 Redis 运行
redis-cli ping

# 2. 构建所有示例工程
cd streammq-samples
mvn clean compile -DskipTests

# 3. 启动 quickstart 示例
cd streammq-sample-quickstart
mvn spring-boot:run
```

---

## 2. GIF 脚本（60 秒版本）

### Scene 1：标题页（0-3 秒）

- **画面**：静态标题卡
- **内容**：`StreamMQ — 让 Redis 成为你的消息总线`
- **操作**：文字淡入
- **标注**：无

### Scene 2：环境展示（3-8 秒）

- **画面**：终端窗口
- **操作**：
  ```bash
  java -version
  redis-cli ping
  ```
- **旁白/字幕**：JDK 21+、Redis 就绪

### Scene 3：创建应用（8-18 秒）

- **画面**：IDE（VS Code）
- **操作**：
  1. 新建 `pom.xml`，粘贴 StreamMQ BOM + starter 依赖
  2. 新建 `application.yml`，配置 streammq + redisson
  3. 新建 `DemoApplication.java`，添加 `@EnableStreamMQ`
- **高亮**：`@EnableStreamMQ` 注解

### Scene 4：发送与消费（18-35 秒）

- **画面**：IDE + 终端分屏
- **操作**：
  1. 编写 `OrderService`（发送消息）
  2. 编写 `OrderConsumer`（消费消息）
  3. 运行应用 → 观察控制台输出
- **关键帧**：
  - `[StreamMQ Demo] 收到订单消息` 日志出现
  - 消息 ID、Keys、Tag、内容完整展示

### Scene 5：高级特性展示（35-50 秒）

- **画面**：代码编辑器
- **操作**（快速切换）：
  1. 事务消息：`template.executeInTransaction(...)`
  2. 延时消息：`delayLevel(DelayLevel.MINUTE_5)`
  3. 顺序消息：`messageModel = MessageModel.ORDERLY`
  4. 死信队列：`dlqMode = true`
- **每个特性**：停留 3-4 秒

### Scene 6：管理端点（50-57 秒）

- **画面**：浏览器
- **操作**：
  1. 访问 `http://localhost:8080/actuator/health`
  2. 访问 `http://localhost:8080/streammq/admin/consumer-groups`
  3. 展示返回的 JSON 数据

### Scene 7：结尾（57-60 秒）

- **画面**：静态结尾卡
- **内容**：`GitHub · streammq/streammq · ⭐ Star`
- **操作**：显示仓库地址和 Star 按钮

---

## 3. 录制规范

### 3.1 帧率与尺寸

| 参数 | 值 |
|------|-----|
| 帧率 | 15 FPS（平衡流畅度与文件大小） |
| 画布尺寸 | 1440×900（16:10） |
| 颜色数 | 256 色（Gifski 自动优化） |
| 最大文件大小 | ≤ 8 MB |
| 时长 | 60 秒 ± 5 秒 |

### 3.2 视觉规范

- **光标**：隐藏或使用大号光标
- **文字大小**：代码 14px，UI 16px+
- **动效**：使用线性淡入淡出，时长 200-300ms
- **颜色方案**：
  - 代码背景：`#1E1E1E`（VS Code Dark+）
  - 主色调：`#4EC9B0`（青色）
  - 强调色：`#CE9178`（橙色，关键字）
- **标注**：使用半透明黄色方框高亮关键区域

### 3.3 禁止事项

- ❌ 不要录制到个人隐私信息（桌面、通知、聊天窗口）
- ❌ 不要展示密码、密钥、Token
- ❌ 不要出现其他品牌 Logo 未经授权
- ❌ 不要使用超过 60 秒的 GIF（注意力经济）
- ❌ 不要在 GIF 中包含大量文字（用户不会细读）

---

## 4. 后期制作

### 4.1 压缩

```bash
# 使用 Gifski（推荐）
gifski --quality 80 --width 1440 --fps 15 -o demo.gif frame*.png

# 使用 ffmpeg（备选）
ffmpeg -i input.mov -vf "fps=15,scale=1440:-1:flags=lanczos,split[s0][s1];[s0]palettegen=stats_mode=diff[p];[s1][p]paletteuse=dither=bayer:bayer_scale=5" output.gif

# 使用 CloudConvert（最简单）
# https://cloudconvert.com
```

### 4.2 优化清单

- [ ] GIF 文件大小 ≤ 8 MB
- [ ] 帧率流畅，无掉帧
- [ ] 文字清晰可读
- [ ] 无明显色彩断层
- [ ] 循环播放（loop forever）
- [ ] 首帧吸引力强（3 秒内抓住注意力）

### 4.3 多平台适配

| 平台 | 推荐格式 | 尺寸 | 时长 |
|------|----------|------|------|
| GitHub README | GIF | 1440×900 | 60s |
| Product Hunt | GIF / MP4 | 1080×1080 | 30s |
| Twitter/X | MP4（GIF 转视频） | 1920×1080 | 30s |
| YouTube Shorts | MP4 | 1080×1920 | 60s |
| 微信公众号 | MP4 | 1080×1080 | 60s |

---

## 5. 发布 Checklist

- [ ] 已在本地验证所有演示步骤可正常运行
- [ ] GIF 在 GitHub README 中加载时间 ≤ 3 秒
- [ ] GIF 在移动端浏览器中可正常播放
- [ ] 已准备静态截图版本（见 [screenshots/README.md](screenshots/README.md)）
- [ ] 已生成短视频版本（MP4，H.264 编码）
- [ ] 已添加字幕/旁白
- [ ] 已校对文字无错别字
- [ ] 仓库 Star 按钮在 GIF 结尾清晰可见

---

## 6. 备选：录屏 + 剪辑方案

如果 GIF 效果不理想，可以使用以下方案录制高质量 MP4：

| 工具 | 平台 | 特点 |
|------|------|------|
| OBS Studio | 全平台 | 免费开源，可添加场景 |
| Screen Studio | macOS | 付费，自动美化 |
| CleanShot X | macOS | 付费，GIF + 视频一体 |
| Snipaste | Windows | 免费，截图 + GIF |

录屏后使用 DaVinci Resolve（免费）剪辑输出 MP4，然后可转换为 GIF。