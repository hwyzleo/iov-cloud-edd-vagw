package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import org.junit.jupiter.api.Test;
import vehicle.common.v1.Envelope;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * keyprov 下行 Envelope 工厂测试（EDD-VAGW-DSN-CR-011 §3.6）。
 */
class KeyprovEnvelopeFactoryTest {

    private final KeyprovEnvelopeFactory factory = new KeyprovEnvelopeFactory(new KeyprovProperties());

    private BrokerAuthenticatedPublishContext context() {
        return new BrokerAuthenticatedPublishContext(KeyprovContextSource.ACL_SESSION_CORRELATED,
                "DEVICE001", "client001", "cert-1", null, 3L, "client001", Instant.now());
    }

    private Envelope.VehicleMessageEnvelope uplink() {
        return Envelope.VehicleMessageEnvelope.newBuilder()
                .setService("security.keyprov")
                .setProtocolVersion("keyprov-v1")
                .setMessageKind(Envelope.MessageKind.MESSAGE_KIND_REQUEST)
                .setDeviceId("DEVICE001")
                .setMessageId("up-msg-001")
                .setRequestId("req-001")
                .setPayloadType("security.keyprov.v1.KeyProvisioningRequest")
                .build();
    }

    @Test
    void buildDownlink_shouldFixRoutingMetadataAndCorrelation() {
        byte[] payload = new byte[]{1, 2, 3};
        Envelope.VehicleMessageEnvelope down = factory.buildDownlink(context(), uplink(),
                "security.keyprov.v1.KeyProvisioningResponse", payload, 1700000000000L);

        assertEquals("security.keyprov", down.getService());
        assertEquals("keyprov-v1", down.getProtocolVersion());
        assertEquals(Envelope.MessageKind.MESSAGE_KIND_RESPONSE, down.getMessageKind());
        assertEquals("req-001", down.getRequestId());
        assertEquals("DEVICE001", down.getDeviceId());
        assertEquals("up-msg-001", down.getCorrelationId());
        assertEquals("security.keyprov.v1.KeyProvisioningResponse", down.getPayloadType());
        assertTrue(down.getExpireAtMs() == 1700000000000L);
        assertFalse(down.getMessageId().isBlank());
        assertNotEquals("up-msg-001", down.getMessageId(), "下行 message_id 必须为新值");
        assertArrayEquals(payload, down.getPayload().toByteArray());
    }

    @Test
    void buildDownlink_withoutTtl_shouldNotSetExpireAtMs() {
        Envelope.VehicleMessageEnvelope down = factory.buildDownlink(context(), uplink(),
                "security.keyprov.v1.KeyProvisioningAckResult", new byte[]{9}, null);
        assertFalse(down.hasExpireAtMs());
    }
}
