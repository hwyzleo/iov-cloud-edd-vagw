package net.hwyz.iov.cloud.edd.vagw.adapter.mqtt;

import net.hwyz.iov.cloud.edd.vagw.application.keyprov.AuthenticatedPublishContextResolver;
import net.hwyz.iov.cloud.edd.vagw.application.keyprov.KeyprovRelayAppService;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov.KeyprovContractGuard;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovRelayResult;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import vehicle.common.v1.Envelope;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * keyprov MQTT 入站适配器测试（EDD-VAGW-DSN-CR-011 §3.1/§11-1/§11-6）。
 */
@ExtendWith(MockitoExtension.class)
class KeyprovMqttIngressAdapterTest {

    @Mock
    private KeyprovContractGuard contractGuard;
    @Mock
    private AuthenticatedPublishContextResolver contextResolver;
    @Mock
    private KeyprovRelayAppService relayService;

    private KeyprovProperties properties;
    private KeyprovMqttIngressAdapter adapter;

    @BeforeEach
    void setUp() {
        properties = new KeyprovProperties();
        properties.setEnabled(true);
        adapter = new KeyprovMqttIngressAdapter(properties, contractGuard, contextResolver, relayService);
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

    @Test
    void handleUplink_invalidTopicFormat_shouldIgnore() {
        adapter.handleUplink("vehicle/DEVICE001/down/keyprov", validEnvelope());
        verifyNoInteractions(relayService, contextResolver);
    }

    @Test
    void handleUplink_disabled_shouldNotRelay() {
        properties.setEnabled(false);
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());
        verifyNoInteractions(relayService, contextResolver);
    }

    @Test
    void handleUplink_contractIncompatible_shouldNotRelay() {
        when(contractGuard.compatible()).thenReturn(false);
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());
        verifyNoInteractions(relayService, contextResolver);
    }

    @Test
    void handleUplink_oversizePayload_shouldNotRelay() {
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", new byte[properties.getMaxEnvelopeBytes() + 1]);
        verifyNoInteractions(relayService, contextResolver);
    }

    @Test
    void handleUplink_unparsableEnvelope_shouldNotRelay() {
        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", new byte[]{0x0A, (byte) 0xFF});
        verifyNoInteractions(relayService, contextResolver);
    }

    @Test
    void handleUplink_contextMissing_shouldNotRelay() {
        when(contractGuard.compatible()).thenReturn(true);
        when(contextResolver.resolve(eq("DEVICE001"), any()))
                .thenReturn(AuthenticatedPublishContextResolver.Resolution.fail(
                        net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING));

        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());

        verifyNoInteractions(relayService);
    }

    @Test
    void handleUplink_valid_shouldRelay() {
        when(contractGuard.compatible()).thenReturn(true);
        when(contextResolver.resolve(eq("DEVICE001"), any()))
                .thenReturn(AuthenticatedPublishContextResolver.Resolution.ok(context()));
        when(relayService.relay(any(), any(), any()))
                .thenReturn(KeyprovRelayResult.delivered("m1", "vehicle/DEVICE001/down/keyprov"));

        adapter.handleUplink("vehicle/DEVICE001/up/keyprov", validEnvelope());

        verify(relayService).relay(any(), any(), any());
    }
}
