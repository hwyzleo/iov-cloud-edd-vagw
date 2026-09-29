package net.hwyz.iov.cloud.edd.vagw.service;

import net.hwyz.iov.cloud.edd.vagw.config.VagwAuthBypassProperties;
import net.hwyz.iov.cloud.edd.vagw.model.dto.MqttAuthRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

/**
 * SuperuserBypassGuard 单元测试（EDD-VAGW-DSN-CR-009 §2.6/§7）
 * 覆盖四因子 AND 校验与逐项缺失的 fail-closed 行为。
 */
@ExtendWith(MockitoExtension.class)
class SuperuserBypassGuardTest {

    @Mock
    private AuthSecurityMetrics metrics;

    private VagwAuthBypassProperties fullProps() {
        VagwAuthBypassProperties props = new VagwAuthBypassProperties();
        props.setEnabled(true);
        props.setPeerHostCidrs(List.of("10.0.0.0/8", "172.16.0.10"));
        props.setServiceCertCn("vagw-service");
        props.setClientIdPrefix("vehicle-access-gateway");
        return props;
    }

    private MqttAuthRequest validRequest() {
        return MqttAuthRequest.builder()
                .peerHost("10.1.2.3:5678")
                .peerCertCn("vagw-service")
                .clientId("vehicle-access-gateway" + "a1b2c3d4")
                .build();
    }

    private SuperuserBypassGuard newGuard(VagwAuthBypassProperties props) {
        return new SuperuserBypassGuard(props, metrics);
    }

    @Test
    void disabled_shouldNeverBypass() {
        VagwAuthBypassProperties props = fullProps();
        props.setEnabled(false);
        assertFalse(newGuard(props).isBypassAllowed(validRequest()));
    }

    @Test
    void emptyPeerHostAllowlist_shouldFailClosed() {
        VagwAuthBypassProperties props = fullProps();
        props.setPeerHostCidrs(List.of());
        assertFalse(newGuard(props).isBypassAllowed(validRequest()));
        verify(metrics).incBypassDenied("no_peerhost_allowlist");
    }

    @Test
    void peerHostNotInAllowlist_shouldFailClosed() {
        VagwAuthBypassProperties props = fullProps();
        MqttAuthRequest request = validRequest();
        request.setPeerHost("192.168.1.1");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("peerhost");
    }

    @Test
    void blankServiceCertCn_shouldFailClosed() {
        VagwAuthBypassProperties props = fullProps();
        props.setServiceCertCn("");
        assertFalse(newGuard(props).isBypassAllowed(validRequest()));
        verify(metrics).incBypassDenied("no_service_cert_cn");
    }

    @Test
    void peerCertCnMismatch_shouldFailClosed() {
        VagwAuthBypassProperties props = fullProps();
        MqttAuthRequest request = validRequest();
        request.setPeerCertCn("some-other-cn");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_cert_cn");
    }

    @Test
    void clientIdPrefixOnly_shouldFailClosed() {
        // 仅前缀、无随机后缀 —— 原实现的提权路径，必须拒绝
        VagwAuthBypassProperties props = fullProps();
        MqttAuthRequest request = validRequest();
        request.setClientId("vehicle-access-gateway");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("clientid");
    }

    @Test
    void fakeClientIdPrefixWithWrongFactors_shouldFailClosed() {
        // 伪造前缀 + peerhost 不在白名单 —— 四因子任一不满足即拒绝
        VagwAuthBypassProperties props = fullProps();
        MqttAuthRequest request = validRequest();
        request.setClientId("vehicle-access-gateway-evil");
        request.setPeerHost("203.0.113.9");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("peerhost");
    }

    @Test
    void allFactorsSatisfied_shouldAllowBypass() {
        assertTrue(newGuard(fullProps()).isBypassAllowed(validRequest()));
    }

    @Test
    void nullRequest_shouldFailClosed() {
        assertFalse(newGuard(fullProps()).isBypassAllowed(null));
    }
}
