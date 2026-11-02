package net.hwyz.iov.cloud.edd.vagw.application.accessinvalidation;

import net.hwyz.iov.cloud.edd.vagw.config.VagwAccessInvalidationProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.EmqxClientKickClient;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.service.BindingService;
import net.hwyz.iov.cloud.edd.vagw.service.InvalidateReason;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import net.hwyz.iov.cloud.sec.viam.api.constant.AccessInvalidationReason;
import net.hwyz.iov.cloud.sec.viam.api.constant.AccessInvalidationType;
import net.hwyz.iov.cloud.sec.viam.api.event.AccessInvalidatedEvent;
import net.hwyz.iov.cloud.sec.viam.api.service.ViamAccessInvalidationService;
import net.hwyz.iov.cloud.sec.viam.api.vo.AccessInvalidationReceiptVo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 访问失效处理器测试（SEC-VIAM-DSN-CR-001 / FR-VIAM-008 / G-3，EDD-VAGW-DSN-CR-019 配套；T019-11）。
 * <p>
 * 覆盖双语义执行（CONNECTION_DISPOSAL/PRINCIPAL_REVOCATION）、generation 判定（不误断新会话）、
 * 乱序版本判定、事件幂等、回执区分状态与回执失败重试。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class AccessInvalidationHandlerTest {

    @Mock
    private SessionService sessionService;
    @Mock
    private BindingService bindingService;
    @Mock
    private EmqxClientKickClient kickClient;
    @Mock
    private StringRedisTemplate redisTemplate;
    @Mock
    private ViamAccessInvalidationService invalidationService;
    @Mock
    private ValueOperations<String, String> valueOperations;

    private VagwAccessInvalidationProperties properties;
    private AccessInvalidationHandler handler;

    @BeforeEach
    void setUp() {
        properties = new VagwAccessInvalidationProperties();
        properties.setReceiptRetryAttempts(3);
        properties.setReceiptRetryBackoff(java.time.Duration.ofMillis(1));
        handler = new AccessInvalidationHandler(sessionService, bindingService, kickClient,
                redisTemplate, invalidationService, properties);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(valueOperations.setIfAbsent(any(), any(), anyLong(), any(java.util.concurrent.TimeUnit.class)))
                .thenReturn(Boolean.TRUE);
        lenient().when(valueOperations.get("vagw:access:version:DEVICE-1")).thenReturn(null);
    }

    private AccessInvalidatedEvent event(AccessInvalidationType type, long securityVersion, Long targetGen) {
        return AccessInvalidatedEvent.builder()
                .eventId("evt-" + securityVersion)
                .principalId("PR-1")
                .hsmUid("DEVICE-1")
                .vin("VIN1")
                .deviceSn("DEVICE-1")
                .invalidationType(type)
                .reason(AccessInvalidationReason.BLOCK)
                .scope("PRINCIPAL")
                .securityVersion(securityVersion)
                .bindingVersion(3L)
                .targetGeneration(targetGen)
                .effectiveAt(Instant.now())
                .effectiveBy(Instant.now().plusSeconds(30))
                .source("MPT_ADMIN")
                .build();
    }

    private SessionInfo session(long generation, boolean online) {
        return SessionInfo.builder()
                .deviceSn("DEVICE-1").vin("VIN1").clientId("client1")
                .online(online).connectionGeneration(generation).brokerSessionId("client1")
                .build();
    }

    @Test
    @DisplayName("CONNECTION_DISPOSAL：targetGeneration 匹配 → 失效+断连+回执 DISCONNECTED")
    void connectionDisposal_generationMatch_shouldInvalidateKickAndReceipt() {
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(session(7L, true)));
        when(kickClient.kick("client1", "ACCESS_INVALIDATED:evt-1")).thenReturn(true);

        handler.handle(event(AccessInvalidationType.CONNECTION_DISPOSAL, 1L, 7L));

        verify(bindingService).invalidate("VIN1", "DEVICE-1", InvalidateReason.SECURITY_BLOCKED);
        verify(kickClient).kick("client1", "ACCESS_INVALIDATED:evt-1");
        verify(sessionService).onDisconnected("DEVICE-1");
        ArgumentCaptor<AccessInvalidationReceiptVo> captor = ArgumentCaptor.forClass(AccessInvalidationReceiptVo.class);
        verify(invalidationService).receipt(captor.capture());
        assertEquals("evt-1", captor.getValue().getInvalidationId());
        assertEquals("DISCONNECTED", captor.getValue().getReceiptState());
        verify(valueOperations).set(eq("vagw:access:version:DEVICE-1"), eq("1"), anyLong(), any(java.util.concurrent.TimeUnit.class));
    }

    @Test
    @DisplayName("CONNECTION_DISPOSAL：generation 已变（新会话）→ SESSION_NOT_FOUND，不误断新会话")
    void connectionDisposal_generationSuperseded_shouldNotKickNewSession() {
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(session(9L, true)));

        handler.handle(event(AccessInvalidationType.CONNECTION_DISPOSAL, 1L, 7L));

        verify(bindingService, never()).invalidate(any(), any(), any());
        verify(kickClient, never()).kick(any(), any());
        verify(sessionService, never()).onDisconnected(any());
        ArgumentCaptor<AccessInvalidationReceiptVo> captor = ArgumentCaptor.forClass(AccessInvalidationReceiptVo.class);
        verify(invalidationService).receipt(captor.capture());
        assertEquals("SESSION_NOT_FOUND", captor.getValue().getReceiptState());
    }

    @Test
    @DisplayName("CONNECTION_DISPOSAL：无在线会话 → SESSION_NOT_FOUND")
    void connectionDisposal_noSession_shouldReceiptSessionNotFound() {
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.empty());

        handler.handle(event(AccessInvalidationType.CONNECTION_DISPOSAL, 1L, 7L));

        ArgumentCaptor<AccessInvalidationReceiptVo> captor = ArgumentCaptor.forClass(AccessInvalidationReceiptVo.class);
        verify(invalidationService).receipt(captor.capture());
        assertEquals("SESSION_NOT_FOUND", captor.getValue().getReceiptState());
    }

    @Test
    @DisplayName("PRINCIPAL_REVOCATION：在线会话 → 失效+断连+回执 DISCONNECTED")
    void principalRevocation_onlineSession_shouldRevokeAndDisconnect() {
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(session(7L, true)));
        when(kickClient.kick("client1", "ACCESS_INVALIDATED:evt-1")).thenReturn(true);

        handler.handle(event(AccessInvalidationType.PRINCIPAL_REVOCATION, 1L, null));

        verify(bindingService).invalidate("VIN1", "DEVICE-1", InvalidateReason.SECURITY_BLOCKED);
        verify(kickClient).kick("client1", "ACCESS_INVALIDATED:evt-1");
        verify(sessionService).onDisconnected("DEVICE-1");
        ArgumentCaptor<AccessInvalidationReceiptVo> captor = ArgumentCaptor.forClass(AccessInvalidationReceiptVo.class);
        verify(invalidationService).receipt(captor.capture());
        assertEquals("DISCONNECTED", captor.getValue().getReceiptState());
    }

    @Test
    @DisplayName("PRINCIPAL_REVOCATION：无在线会话 → 缓存/ACL 失效 + 回执 CACHE_ACL_INVALIDATED")
    void principalRevocation_noSession_shouldReceiptCacheAclInvalidated() {
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.empty());

        handler.handle(event(AccessInvalidationType.PRINCIPAL_REVOCATION, 1L, null));

        verify(bindingService).invalidate("VIN1", "DEVICE-1", InvalidateReason.SECURITY_BLOCKED);
        ArgumentCaptor<AccessInvalidationReceiptVo> captor = ArgumentCaptor.forClass(AccessInvalidationReceiptVo.class);
        verify(invalidationService).receipt(captor.capture());
        assertEquals("CACHE_ACL_INVALIDATED", captor.getValue().getReceiptState());
        verify(kickClient, never()).kick(any(), any());
    }

    @Test
    @DisplayName("乱序旧事件（securityVersion ≤ 已处理版本）→ 忽略，不误断新会话、不回执")
    void oldSecurityVersion_shouldBeIgnored() {
        when(valueOperations.get("vagw:access:version:DEVICE-1")).thenReturn("5");

        handler.handle(event(AccessInvalidationType.PRINCIPAL_REVOCATION, 3L, null));

        verify(bindingService, never()).invalidate(any(), any(), any());
        verify(invalidationService, never()).receipt(any());
        verify(sessionService, never()).onDisconnected(any());
    }

    @Test
    @DisplayName("事件丢失场景：会话授权版本 ≥ 迟到事件版本 → STALE_IGNORED，不误断已恢复重连的新会话")
    void sessionAuthorizedAfterLostEvent_shouldNotKickNewSession() {
        // block(v10) 事件在 Kafka 丢失 → unblock(v11) 后设备重连建立新会话（authorized=11）
        // 迟到 block(v10) 事件重投：会话授权版本 11 ≥ 事件版本 10 → 不得断连（仅靠 lastSeen 无法覆盖）
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(SessionInfo.builder()
                        .deviceSn("DEVICE-1").vin("VIN1").clientId("client1")
                        .online(true).connectionGeneration(7L).brokerSessionId("client1")
                        .authorizedSecurityVersion(11L)
                        .build()));

        handler.handle(event(AccessInvalidationType.PRINCIPAL_REVOCATION, 10L, null));

        verify(bindingService, never()).invalidate(any(), any(), any());
        verify(kickClient, never()).kick(any(), any());
        verify(sessionService, never()).onDisconnected(any());
        ArgumentCaptor<AccessInvalidationReceiptVo> captor = ArgumentCaptor.forClass(AccessInvalidationReceiptVo.class);
        verify(invalidationService).receipt(captor.capture());
        assertEquals("STALE_IGNORED", captor.getValue().getReceiptState());
    }

    @Test
    @DisplayName("会话授权版本早于事件版本（正常撤销）→ 仍断连（不因重连换 generation 逃逸）")
    void sessionAuthorizedBeforeEvent_shouldRevoke() {
        when(sessionService.getSessionByDeviceSn("DEVICE-1"))
                .thenReturn(Optional.of(SessionInfo.builder()
                        .deviceSn("DEVICE-1").vin("VIN1").clientId("client1")
                        .online(true).connectionGeneration(7L).brokerSessionId("client1")
                        .authorizedSecurityVersion(9L)
                        .build()));
        when(kickClient.kick("client1", "ACCESS_INVALIDATED:evt-10")).thenReturn(true);

        handler.handle(event(AccessInvalidationType.PRINCIPAL_REVOCATION, 10L, null));

        verify(bindingService).invalidate("VIN1", "DEVICE-1", InvalidateReason.SECURITY_BLOCKED);
        verify(kickClient).kick("client1", "ACCESS_INVALIDATED:evt-10");
        ArgumentCaptor<AccessInvalidationReceiptVo> captor = ArgumentCaptor.forClass(AccessInvalidationReceiptVo.class);
        verify(invalidationService).receipt(captor.capture());
        assertEquals("DISCONNECTED", captor.getValue().getReceiptState());
    }

    @Test
    @DisplayName("事件幂等：重复 eventId → 跳过执行与回执")
    void duplicateEventId_shouldBeSkipped() {
        when(valueOperations.setIfAbsent(any(), any(), anyLong(), any(java.util.concurrent.TimeUnit.class)))
                .thenReturn(Boolean.FALSE);

        handler.handle(event(AccessInvalidationType.PRINCIPAL_REVOCATION, 1L, null));

        verify(bindingService, never()).invalidate(any(), any(), any());
        verify(invalidationService, never()).receipt(any());
    }

    @Test
    @DisplayName("回执失败 → 有限重试后告警（VIAM 对账兜底），不伪造完成")
    void receiptFailure_shouldRetryBounded() {
        when(sessionService.getSessionByDeviceSn("DEVICE-1")).thenReturn(Optional.empty());
        doThrow(new RuntimeException("receipt down")).when(invalidationService).receipt(any());

        handler.handle(event(AccessInvalidationType.PRINCIPAL_REVOCATION, 1L, null));

        // 执行已完成（缓存失效），回执重试 3 次仍失败（不抛异常阻断）
        verify(bindingService).invalidate("VIN1", "DEVICE-1", InvalidateReason.SECURITY_BLOCKED);
        verify(invalidationService, times(3)).receipt(any());
    }
}
