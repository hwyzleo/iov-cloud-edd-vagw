package net.hwyz.iov.cloud.edd.vagw.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.VehicleMessageDownlinkPublisher;
import net.hwyz.iov.cloud.edd.vagw.application.AccessIdentityValidator;
import net.hwyz.iov.cloud.edd.vagw.application.GatewayDeliveryService;
import net.hwyz.iov.cloud.edd.vagw.application.VehicleEnvelopeValidator;
import net.hwyz.iov.cloud.edd.vagw.application.VehicleMessageBridgeService;
import net.hwyz.iov.cloud.edd.vagw.config.VagwBindingProperties;
import net.hwyz.iov.cloud.edd.vagw.config.VagwFotaTopicProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.ViamResolveResult;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.ViamVehicleAccessIdentityClient;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.idempotency.VehicleBridgeInbox;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka.EnvelopeDlqPublisher;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.kafka.EnvelopeKafkaProducer;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.route.VehicleRouteCatalog;
import net.hwyz.iov.cloud.edd.vagw.model.binding.BindingSource;
import net.hwyz.iov.cloud.edd.vagw.model.binding.VehicleAccessBinding;
import net.hwyz.iov.cloud.edd.vagw.model.enums.ErrorCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;
import vehicle.common.v1.Envelope;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * 上行绑定上下文自愈全链路集成测试（EDD-VAGW-DSN-CR-020 §9）。
 * <p>
 * 串联 real {@link BindingServiceImpl}（三级解析 + 会话续期/清理）与 real
 * {@link VehicleMessageBridgeService}（契约/身份/绑定/幂等/桥接），用内存 Map 模拟 Redis
 * 缓存状态、用可变应答模拟 VIAM 绑定投影，不依赖真实 Redis/Kafka/VIAM。
 * </p>
 * <p>
 * 覆盖：TTL 到期自愈 → 断连清理后再次自愈 → 改绑后旧设备仍被拒（binding_conflict）；
 * 断言常态 {@code binding_context_missing} 为 0，且拒绝只进 DLQ、不产生 Kafka 业务结果。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("上行绑定自愈全链路集成测试")
class BindingUplinkResolvePathIntegrationTest {

    private static final String HSM = "DEVICE001";
    private static final String VIN = "VIN-A";
    private static final String HSM_KEY = "vagw:binding:hsm:" + HSM;
    private static final String VIN_KEY = "vagw:binding:vin:" + VIN;
    private static final String ANCHOR_KEY = "vagw:binding:hsm-anchor:" + HSM;

    @Mock
    private StringRedisTemplate redisTemplate;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private ViamVehicleAccessIdentityClient viamClient;
    @Mock
    private AccessIdentityValidator accessIdentityValidator;
    @Mock
    private VehicleEnvelopeValidator envelopeValidator;
    @Mock
    private SessionService sessionService;
    @Mock
    private VehicleBridgeInbox bridgeInbox;
    @Mock
    private EnvelopeKafkaProducer kafkaProducer;
    @Mock
    private EnvelopeDlqPublisher dlqPublisher;
    @Mock
    private VehicleMessageDownlinkPublisher downlinkPublisher;
    @Mock
    private GatewayDeliveryService gatewayDeliveryService;
    @Mock
    private VagwMqttSubscriptionMetrics mqttMetrics;

    private final Map<String, String> cache = new HashMap<>();
    private final AtomicReference<ViamResolveResult> viamProjection = new AtomicReference<>();

    private ObjectMapper objectMapper;
    private VagwBindingProperties properties;
    private BindingResolutionMetrics metrics;
    private final VehicleRouteCatalog routeCatalog =
            new VehicleRouteCatalog(new VagwFotaTopicProperties());
    private BindingServiceImpl bindingService;
    private VehicleMessageBridgeService bridgeService;

    @BeforeEach
    void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        properties = new VagwBindingProperties();
        metrics = new BindingResolutionMetrics();
        bindingService = new BindingServiceImpl(redisTemplate, viamClient, objectMapper,
                properties, metrics);
        bindingService.initUplinkResolve();

        bridgeService = new VehicleMessageBridgeService(
                accessIdentityValidator, envelopeValidator, bindingService, sessionService,
                bridgeInbox, kafkaProducer, dlqPublisher, downlinkPublisher,
                gatewayDeliveryService, routeCatalog, mqttMetrics);
        ReflectionTestUtils.setField(bridgeService, "uplinkProduceRetries", 3);
        ReflectionTestUtils.setField(bridgeService, "uplinkProduceRetryDelayMs", 0L);

        // Redis 缓存替身：get/setIfAbsent/expire/getExpire/delete 直接作用于内存 Map
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(valueOperations.get(anyString()))
                .thenAnswer(inv -> cache.get(inv.<String>getArgument(0)));
        lenient().when(valueOperations.setIfAbsent(anyString(), anyString(), anyLong(),
                        any(TimeUnit.class)))
                .thenAnswer(inv -> cache.putIfAbsent(inv.getArgument(0), inv.getArgument(1)) == null);
        lenient().when(redisTemplate.execute(any(RedisCallback.class))).thenReturn(null);
        lenient().when(redisTemplate.expire(anyString(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(inv -> cache.containsKey(inv.<String>getArgument(0)));
        lenient().when(redisTemplate.getExpire(anyString(), any(TimeUnit.class)))
                .thenAnswer(inv -> cache.containsKey(inv.<String>getArgument(0)) ? 1800L : -2L);
        lenient().when(redisTemplate.delete(anyString()))
                .thenAnswer(inv -> cache.remove(inv.<String>getArgument(0)) != null);

        // 桥接门禁与桥接下游
        lenient().when(envelopeValidator.validateUplink(any(), any())).thenReturn(null);
        lenient().when(accessIdentityValidator.validate(anyString(), anyString())).thenReturn(null);
        lenient().when(bridgeInbox.findFinal(any(), anyString())).thenReturn(Optional.empty());
        lenient().when(kafkaProducer.sendUplink(any(), any(), any(), any(), any()))
                .thenReturn(new EnvelopeKafkaProducer.SendResult(0, 1));
        lenient().when(viamClient.resolveByVin(anyString())).thenAnswer(inv -> viamProjection.get());

        // 准入已写入的会话级双向映射
        seedCache(binding(VIN, HSM, 1L));
        viamProjection.set(ViamResolveResult.resolved(VIN, HSM, "PR-1", "TBOX-1", 1L, Instant.now()));
    }

    @Test
    @DisplayName("TTL 到期 → VIAM 回源自愈 → 断连清理后再次自愈 → 改绑后拒绝")
    void uplinkBindingSelfHealPath() throws Exception {
        // 1) 缓存命中：直接放行，不回源
        assertTrue(uplink("msg-1").ok());
        verifyNoInteractions(viamClient);

        // 2) 模拟缓存 TTL 到期（hsm 方向 key 消失）→ 上行自愈：回源 VIAM 并放行
        cache.remove(HSM_KEY);
        assertTrue(uplink("msg-2").ok());
        verify(viamClient, times(1)).resolveByVin(VIN);
        assertTrue(cache.containsKey(ANCHOR_KEY), "回源成功后应建立单会话上限锚点");

        // 3) 断连清理会话级映射（hsm 方向 + 锚点），VIN 方向保留供下行解析
        bindingService.clearSessionMapping(HSM);
        assertFalse(cache.containsKey(HSM_KEY));
        assertFalse(cache.containsKey(ANCHOR_KEY));
        assertTrue(cache.containsKey(VIN_KEY), "断连清理不得删除 VIN 方向映射");

        // 4) 清理后上行再次自愈（会话级映射已被清理，不依赖重连）
        assertTrue(uplink("msg-3").ok());
        verify(viamClient, times(2)).resolveByVin(VIN);

        // 5) VIN 已改绑到其它设备：旧设备上行必须被拒，且失效双向映射
        viamProjection.set(ViamResolveResult.resolved(VIN, "DEVICE-OTHER", "PR-1", "TBOX-1", 2L,
                Instant.now()));
        VehicleMessageBridgeService.UplinkResult conflict = uplink("msg-4");

        assertFalse(conflict.ok());
        assertEquals(ErrorCode.BINDING_CONFLICT, conflict.errorCode());
        verify(dlqPublisher).publish(eq(routeCatalog.upDlqTopic()), eq(HSM),
                any(byte[].class), contains("binding_conflict"));
        assertFalse(cache.containsKey(VIN_KEY), "改绑冲突必须原子失效 VIN 方向映射");

        // 6) 全程：Kafka 只桥接前 3 条；常态 binding_context_missing 必须为 0
        verify(kafkaProducer, times(3)).sendUplink(any(), any(), any(), any(), any());
        assertEquals(0L, metrics.get("vagw_binding_uplink_resolve_total{result=REJECTED,"
                + "reason=context_missing}"));
        assertTrue(metrics.get("vagw_binding_uplink_resolve_total{result=RESOLVED,"
                + "reason=viam_resolve}") >= 2L);
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private VehicleMessageBridgeService.UplinkResult uplink(String messageId) {
        Envelope.VehicleMessageEnvelope envelope = Envelope.VehicleMessageEnvelope.newBuilder()
                .setRequestId("req-" + messageId)
                .setTimestampMs(System.currentTimeMillis())
                .setProtocolVersion("fota-v1")
                .setDeviceId(HSM)
                .setVin(VIN)
                .setMessageId(messageId)
                // 清单上报：与其他 fota 上行等价受检（不存在按报文类型的豁免）
                .setPayloadType("vehicle.fota.v1.ManifestReport")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setService("vehicle.fota")
                .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{1, 2, 3}))
                .build();
        return bridgeService.processUplink(envelope.toByteArray(), HSM);
    }

    private void seedCache(VehicleAccessBinding binding) throws Exception {
        String json = objectMapper.writeValueAsString(binding);
        cache.put(HSM_KEY, json);
        cache.put(VIN_KEY, json);
    }

    private VehicleAccessBinding binding(String vin, String hsmUid, long version) {
        return VehicleAccessBinding.builder()
                .vin(vin)
                .hsmUid(hsmUid)
                .tboxSn("TBOX-1")
                .bindingVersion(version)
                .bindingUpdatedAt(Instant.now())
                .source(BindingSource.VIAM_ADMISSION)
                .build();
    }
}
