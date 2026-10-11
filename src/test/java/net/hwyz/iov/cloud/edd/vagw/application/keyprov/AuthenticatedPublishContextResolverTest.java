package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * 已认证发布上下文解析测试（EDD-VAGW-DSN-CR-011 §3.2，ACL_SESSION_CORRELATED）。
 */
@ExtendWith(MockitoExtension.class)
class AuthenticatedPublishContextResolverTest {

    @Mock
    private SessionService sessionService;

    @InjectMocks
    private AuthenticatedPublishContextResolver resolver;

    private SessionInfo session(boolean online, long generation) {
        return SessionInfo.builder()
                .deviceSn("DEVICE001")
                .clientId("client001")
                .online(online)
                .connectionGeneration(generation)
                .certSerial("cert-1")
                .certFingerprint("fp-1")
                .build();
    }

    @Test
    void resolve_missingSession_shouldFailWithContextMissing() {
        when(sessionService.getSessionByDeviceSn("DEVICE001")).thenReturn(Optional.empty());
        var resolution = resolver.resolve("device001", Instant.now());
        assertNull(resolution.context());
        assertEquals(KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING, resolution.failureReason());
    }

    @Test
    void resolve_offlineSession_shouldFailWithStale() {
        when(sessionService.getSessionByDeviceSn("DEVICE001")).thenReturn(Optional.of(session(false, 1L)));
        var resolution = resolver.resolve("DEVICE001", Instant.now());
        assertNull(resolution.context());
        assertEquals(KeyprovReason.KEYPROV_SESSION_STALE, resolution.failureReason());
    }

    @Test
    void resolve_blankTopicHsmUid_shouldFailWithContextMissing() {
        var resolution = resolver.resolve("  ", Instant.now());
        assertNull(resolution.context());
        assertEquals(KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING, resolution.failureReason());
    }

    @Test
    void resolve_onlineSession_shouldBuildAclSessionCorrelatedContext() {
        when(sessionService.getSessionByDeviceSn("DEVICE001")).thenReturn(Optional.of(session(true, 5L)));
        Instant receivedAt = Instant.now();

        var resolution = resolver.resolve("device001", receivedAt);

        assertNotNull(resolution.context());
        assertEquals(KeyprovContextSource.ACL_SESSION_CORRELATED, resolution.context().source());
        assertEquals("DEVICE001", resolution.context().hsmUid());
        assertEquals("client001", resolution.context().clientId());
        assertEquals("cert-1", resolution.context().certSerial());
        assertEquals("fp-1", resolution.context().certFingerprint());
        assertEquals(5L, resolution.context().connectionGeneration());
        assertEquals(receivedAt, resolution.context().receivedAt());
    }
}
