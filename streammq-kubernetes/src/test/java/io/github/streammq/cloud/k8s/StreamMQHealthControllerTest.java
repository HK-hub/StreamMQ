package io.github.streammq.cloud.k8s;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * {@link StreamMQHealthController} 单元测试，验证 K8s 存活与就绪探针端点。
 *
 * <p>使用 {@link WebMvcTest} 加载 Web 层，通过 {@link MockBean} 注入模拟的 {@link StreamMQListenerContainer}，使用
 * MockMvc 验证 JSON 响应。 容器不存在时的降级场景使用独立 MockMvc 测试。
 */
@DisplayName("健康探针控制器测试")
@WebMvcTest(StreamMQHealthController.class)
class StreamMQHealthControllerTest {

  @Autowired private MockMvc mockMvc;

  @MockBean private StreamMQListenerContainer container;

  @Test
  @DisplayName("liveness - 始终返回 UP 状态与 backend 字段")
  void liveness_returnsUpStatus() throws Exception {
    mockMvc
        .perform(get("/streammq/health/liveness"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("UP"))
        .andExpect(jsonPath("$.backend").value("unknown"));
  }

  @Test
  @DisplayName("readiness - 容器运行中时返回 ready=true")
  void readiness_whenRunning_returnsReadyTrue() throws Exception {
    when(container.isRunning()).thenReturn(true);
    when(container.getConsumers()).thenReturn(Collections.emptyList());
    mockMvc
        .perform(get("/streammq/health/readiness"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ready").value(true))
        .andExpect(jsonPath("$.consumerCount").value(0));
  }

  @Test
  @DisplayName("readiness - 容器未运行时返回 ready=false")
  void readiness_whenNotRunning_returnsReadyFalse() throws Exception {
    when(container.isRunning()).thenReturn(false);
    when(container.getConsumers()).thenReturn(Collections.emptyList());
    mockMvc
        .perform(get("/streammq/health/readiness"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ready").value(false))
        .andExpect(jsonPath("$.consumerCount").value(0));
  }

  @Test
  @DisplayName("readiness - 返回正确的消费者数量")
  void readiness_returnsCorrectConsumerCount() throws Exception {
    when(container.isRunning()).thenReturn(true);
    when(container.getConsumers())
        .thenReturn(
            List.of(
                new StreamMQListenerContainer.ConsumerMetadata(
                    "order-topic", "order-group", Object.class, Object.class),
                new StreamMQListenerContainer.ConsumerMetadata(
                    "payment-topic", "payment-group", Object.class, Object.class)));
    mockMvc
        .perform(get("/streammq/health/readiness"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ready").value(true))
        .andExpect(jsonPath("$.consumerCount").value(2));
  }

  @Test
  @DisplayName("liveness - 响应包含 status 与 backend 两个键")
  void liveness_responseContainsBothFields() throws Exception {
    mockMvc
        .perform(get("/streammq/health/liveness"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$").isMap())
        .andExpect(jsonPath("$.status").exists())
        .andExpect(jsonPath("$.backend").exists());
  }

  @Test
  @DisplayName("readiness - 容器不存在时返回降级状态")
  @SuppressWarnings({"rawtypes", "unchecked"})
  void readiness_whenContainerNotAvailable_returnsDegraded() throws Exception {
    ObjectProvider<StreamMQListenerContainer> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(null);
    StreamMQHealthController controller = new StreamMQHealthController(provider);
    MockMvc standaloneMockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    standaloneMockMvc
        .perform(get("/streammq/health/readiness"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ready").value(false))
        .andExpect(jsonPath("$.consumerCount").value(0));
  }
}
