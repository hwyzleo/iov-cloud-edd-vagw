package net.hwyz.iov.cloud.edd.vagw.controller;

import net.hwyz.iov.cloud.edd.vagw.model.dto.VehicleStatusResponse;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * GET /v1/vehicles/{vin}/status 时间契约（EDD-VAGW-DSN-CR-018 §5.2.1 "实施前核验"）。
 * <p>
 * 核验结论（2026-10-28，commit：本 CR 实现提交）：
 * <ul>
 *   <li>Java 类型：{@code VehicleStatusResponse.lastOnlineAt/lastOfflineAt} 为 {@code String}；
 *       控制器以 {@code Instant.toString()} 预格式化（ISO-8601 含 Z，绝对时刻），<b>不经
 *       java.time JSON mapper</b>，不受 date-format 开关影响；</li>
 *   <li>真实路由：GET /v1/vehicles/{vin}/status（VehicleStatusController）；</li>
 *   <li>wire 格式：非空值输出含 Z 的 ISO String，无双重编码；空值策略不变（缺失时字段为 null，
 *       非空串替代）。</li>
 * </ul>
 * 该路径登记为既有 ISO 字符串契约，兼容阶段毫秒写出仅指受管 java.time 字段，不要求本路径
 * 改回数字。
 * </p>
 */
@DisplayName("车辆状态查询时间契约测试（CR-018）")
class VagwStatusPathContractTest {

    private static final Instant CONNECTED = Instant.parse("2026-10-28T12:43:32.823Z");

    @Test
    @DisplayName("在线会话：last_online_at 为含 Z 的 ISO String，无双重编码")
    void onlineSession_lastOnlineAtIsoString() throws Exception {
        SessionService sessionService = mock(SessionService.class);
        SessionInfo session = SessionInfo.builder()
                .deviceSn("DEVICE-1").vin("VIN1").online(true)
                .connectedAt(CONNECTED).disconnectedAt(null)
                .build();
        when(sessionService.getSessionByVin("VIN1")).thenReturn(Optional.of(session));

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new VehicleStatusController(sessionService)).build();
        mockMvc.perform(get("/v1/vehicles/VIN1/status"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                // 非空值含 Z（绝对时刻 offset），且为字符串而非数字
                .andExpect(jsonPath("$.lastOnlineAt").value(CONNECTED.toString()))
                .andExpect(jsonPath("$.lastOfflineAt").doesNotExist());
    }

    @Test
    @DisplayName("离线会话：空值策略不变（字段省略/为 null，不以空串代替）")
    void offlineSession_nullPolicyUnchanged() throws Exception {
        SessionService sessionService = mock(SessionService.class);
        when(sessionService.getSessionByVin("VIN1")).thenReturn(Optional.empty());

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new VehicleStatusController(sessionService)).build();
        mockMvc.perform(get("/v1/vehicles/VIN1/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.online").value(false))
                .andExpect(jsonPath("$.lastOnlineAt").doesNotExist())
                .andExpect(jsonPath("$.lastOfflineAt").doesNotExist());
    }

    @Test
    @DisplayName("响应 DTO：lastOnlineAt/lastOfflineAt 保持 String 类型（非 Instant，不增加注解）")
    void responseDto_stringTypeWithoutAnnotations() throws Exception {
        java.lang.reflect.Field lastOnlineAt = VehicleStatusResponse.class.getDeclaredField("lastOnlineAt");
        java.lang.reflect.Field lastOfflineAt = VehicleStatusResponse.class.getDeclaredField("lastOfflineAt");
        org.junit.jupiter.api.Assertions.assertEquals(String.class, lastOnlineAt.getType());
        org.junit.jupiter.api.Assertions.assertEquals(String.class, lastOfflineAt.getType());
        org.junit.jupiter.api.Assertions.assertEquals(0, lastOnlineAt.getAnnotations().length);
        org.junit.jupiter.api.Assertions.assertEquals(0, lastOfflineAt.getAnnotations().length);
    }
}
