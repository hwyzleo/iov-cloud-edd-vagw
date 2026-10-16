package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import net.hwyz.iov.cloud.edd.vagw.application.keyprov.AuthenticatedPublishContextResolver;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.KeyprovProtocolValidator;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.KeyprovRelayAppService;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovAdmissionBulkhead;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovExecutor;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovRelayResult;
import net.hwyz.iov.cloud.edd.vagw.service.KeyprovMetrics;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vehicle.common.v1.Envelope;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * keyprov MQTT 入站适配器测试（EDD-VAGW-DSN-CR-011 §3.1，由 EDD-VAGW-DSN-CR-016 §2.1/§7 增量修订）。
 * <p>
 * 验证 ingress 只做门禁/取许可/提交，不在 MQTT 回调内阻塞等待 VMD；饱和与拒绝在回调内立即返回。
 * </p>
 */
@ExtendWith(MockitoExtension.class)
class KeyprovMqttIngressAdapterTest {

    @Mock
    private KeyprovContractGuard contractGuard;
    @Mock
    private KeyprovProtocolValidator protocolValidator;
    @Mock
    private AuthenticatedPublishContextResolver contextResolver;
    @Mock
    private KeyprovRelayAppService relayService;

    private KeyprovProperties properties;
    private KeyprovAdmissionBulkhead bulkhead;
    private KeyprovExecutor executor;
    private KeyprovMqttIngressAdapter adapter;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        properties.setEnabled(true);
        properties.setMaxInFlight(2);
        properties.setQueueCapacity(0);
        KeyprovMetrics metrics = new KeyprovMetrics();
        bulkhead = new KeyprovAdmissionBulkhead(properties, metrics);
        executor = new KeyprovExecutor(properties, metrics);
        adapter = new KeyprovMqttIngressAdapter(properties, contractGuard, protocolValidator, contextResolver,
                bulkhead, executor, relayService, new VagwMqttSubscriptionMetrics());
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
    }

    private byte[] validEnvelope() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov").setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001").setMessageId("m1").setRequestId("r1")
                .setPayloadType("security.keyprov.v1.KeyProvisioningRequest")
                .setPayload(KeyProvisioningRequest.newBuilder().setRequestId("r1").build().toByteString())
                .build().toByteArray();
    }

    private BrokerAuthenticatedPublishContext context() {
        return new BrokerAuthenticatedPublishContext(KeyprovContextSource.ACL_SESSION_CORRELATED,
                "DEVICE001", "client001", null, null, 1L, "client001", Instant.now());
    }

    private void awaitExecutorIdle(long baseline) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            if (executor.activeCount() == 0 && executor.completedTaskCount() >= baseline + 1) {
                return;
            }
            Thread.sleep(10L);
        }
        fail("keyprov executor did not complete the submitted task in time");
    }

    @Test
    void handleUplink_invalidTopicFormat_shouldIgnore() {
        adapter.handleUplink("vehicle/DEVICE001/down/keyprov", validEnvelope());
        verifyNoInteractions(relayService, contextResolver, protocolValidator);
    }

    @Test
    void handleUplink_disabled_shouldNotRelay() {
        properties.setEnabled(false);
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());
        verifyNoInteractions(relayService, contextResolver, protocolValidator);
    }

    @Test
    void handleUplink_contractIncompatible_shouldNotRelay() {
        when(contractGuard.compatible()).thenReturn(false);
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());
        verifyNoInteractions(relayService, contextResolver, protocolValidator);
    }

    @Test
    void handleUplink_oversizePayload_shouldNotRelay() {
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", new byte[properties.getMaxEnvelopeBytes() + 1]);
        verifyNoInteractions(relayService, contextResolver, protocolValidator);
    }

    @Test
    void handleUplink_unparsableEnvelope_shouldNotRelay() {
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", new byte[]{0x0A, (byte) 0xFF});
        verifyNoInteractions(relayService, contextResolver, protocolValidator);
    }

    @Test
    void handleUplink_contextMissing_shouldNotRelay() {
        when(contractGuard.compatible()).thenReturn(true);
        when(contextResolver.resolve(eq("DEVICE001"), any()))
                .thenReturn(AuthenticatedPublishContextResolver.Resolution.fail(
                        KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING));

        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());

        verifyNoInteractions(relayService, protocolValidator);
    }

    @Test
    void handleUplink_protocolRejected_shouldNotSubmit() {
        when(contractGuard.compatible()).thenReturn(true);
        when(contextResolver.resolve(eq("DEVICE001"), any()))
                .thenReturn(AuthenticatedPublishContextResolver.Resolution.ok(context()));
        when(protocolValidator.validateUplink(any(), any(), any()))
                .thenReturn(new KeyprovProtocolValidator.Failure(KeyprovReason.KEYPROV_PAYLOAD_INVALID, "bad"));

        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());

        verifyNoInteractions(relayService);
    }

    @Test
    void handleUplink_valid_shouldSubmitAndReturnWithoutWaiting() throws Exception {
        when(contractGuard.compatible()).thenReturn(true);
        when(contextResolver.resolve(eq("DEVICE001"), any()))
                .thenReturn(AuthenticatedPublishContextResolver.Resolution.ok(context()));
        when(protocolValidator.validateUplink(any(), any(), any())).thenReturn(null);
        when(relayService.relay(any(), any(), any()))
                .thenReturn(KeyprovRelayResult.delivered("m1", "vehicle/DEVICE001/down/keyprov"));

        long baseline = executor.completedTaskCount();
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());
        awaitExecutorIdle(baseline);

        verify(relayService).relay(any(), any(), any());
    }

    @Test
    void handleUplink_bulkheadSaturated_shouldRejectImmediatelyWithoutSubmitting() {
        when(contractGuard.compatible()).thenReturn(true);
        when(contextResolver.resolve(eq("DEVICE001"), any()))
                .thenReturn(AuthenticatedPublishContextResolver.Resolution.ok(context()));
        when(protocolValidator.validateUplink(any(), any(), any())).thenReturn(null);

        // 占满唯一许可，模拟已有 in-flight
        KeyprovAdmissionBulkhead.Permit held = bulkhead.tryAcquire();
        try {
            adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());
            verifyNoInteractions(relayService);
        } finally {
            held.close();
        }
    }
}
