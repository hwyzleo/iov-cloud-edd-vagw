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
 * SuperuserBypassGuard 单元测试（EDD-VAGW-DSN-CR-009 §2.6/§7 + EDD-VAGW-DSN-CR-010 §8）
 * 覆盖四因子 AND 校验、服务凭据双模式（mTLS 证书 CN / 明文账号）、启动配置校验
 * 与逐项缺失的 fail-closed 行为。
 */
@ExtendWith(MockitoExtension.class)
class SuperuserBypassGuardTest {

    @Mock
    private AuthSecurityMetrics metrics;

    /** mTLS 模式：专属服务证书 CN 因子 */
    private VagwAuthBypassProperties certModeProps() {
        VagwAuthBypassProperties props = new VagwAuthBypassProperties();
        props.setEnabled(true);
        props.setPeerHostCidrs(List.of("10.0.0.0/8", "172.16.0.10"));
        props.setServiceCertCn("vagw-service");
        props.setClientIdPrefix("vehicle-access-gateway");
        return props;
    }

    /** 明文账号模式：专属服务账号因子（EDD-VAGW-DSN-CR-010） */
    private VagwAuthBypassProperties accountModeProps() {
        VagwAuthBypassProperties props = certModeProps();
        props.setServiceCertCn("");
        props.setServiceUsername("vagw-service");
        props.setServicePassword("s3cret-pass");
        return props;
    }

    /** mTLS 模式下的合法请求（带证书 CN、无账号） */
    private MqttAuthRequest certRequest() {
        return MqttAuthRequest.builder()
                .peerHost("10.1.2.3:5678")
                .peerCertCn("vagw-service")
                .clientId("vehicle-access-gateway" + "a1b2c3d4")
                .build();
    }

    /** 明文账号模式下的合法请求 */
    private MqttAuthRequest accountRequest(String username, String password) {
        return MqttAuthRequest.builder()
                .peerHost("10.1.2.3:5678")
                .username(username)
                .password(password)
                .clientId("vehicle-access-gateway" + "a1b2c3d4")
                .build();
    }

    private SuperuserBypassGuard newGuard(VagwAuthBypassProperties props) {
        return new SuperuserBypassGuard(props, metrics);
    }

    // ---------- 总开关 / 启动配置校验 ----------

    @Test
    void disabled_shouldNeverBypass() {
        VagwAuthBypassProperties props = accountModeProps();
        props.setEnabled(false);
        assertFalse(newGuard(props).isBypassAllowed(accountRequest("vagw-service", "s3cret-pass")));
    }

    @Test
    void nullRequest_shouldFailClosed() {
        assertFalse(newGuard(accountModeProps()).isBypassAllowed(null));
    }

    @Test
    void enabledWithNoCredentialMode_shouldFailFast() {
        VagwAuthBypassProperties props = accountModeProps();
        props.setServiceCertCn("");
        props.setServiceUsername("");
        props.setServicePassword("");
        SuperuserBypassGuard guard = newGuard(props);
        assertThrows(IllegalStateException.class, guard::validateConfig);
    }

    @Test
    void enabledWithUsernameOnly_shouldFailFast() {
        VagwAuthBypassProperties props = accountModeProps();
        props.setServicePassword("");
        SuperuserBypassGuard guard = newGuard(props);
        assertThrows(IllegalStateException.class, guard::validateConfig);
    }

    @Test
    void enabledWithEmptyPeerHostAllowlist_shouldFailFast() {
        VagwAuthBypassProperties props = accountModeProps();
        props.setPeerHostCidrs(List.of());
        SuperuserBypassGuard guard = newGuard(props);
        assertThrows(IllegalStateException.class, guard::validateConfig);
    }

    @Test
    void enabledWithCompleteAccountMode_shouldPassValidation() {
        assertDoesNotThrow(() -> newGuard(accountModeProps()).validateConfig());
    }

    @Test
    void enabledWithCompleteCertMode_shouldPassValidation() {
        assertDoesNotThrow(() -> newGuard(certModeProps()).validateConfig());
    }

    // ---------- 因子 1：peerhost ----------

    @Test
    void emptyPeerHostAllowlist_shouldFailClosed() {
        VagwAuthBypassProperties props = accountModeProps();
        props.setPeerHostCidrs(List.of());
        assertFalse(newGuard(props).isBypassAllowed(accountRequest("vagw-service", "s3cret-pass")));
        verify(metrics).incBypassDenied("no_peerhost_allowlist");
    }

    @Test
    void peerHostNotInAllowlist_shouldFailClosed() {
        VagwAuthBypassProperties props = accountModeProps();
        MqttAuthRequest request = accountRequest("vagw-service", "s3cret-pass");
        request.setPeerHost("192.168.1.1");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("peerhost");
    }

    // ---------- 因子 2：mTLS 证书 CN 模式 ----------

    @Test
    void certMode_noCredentialModeConfigured_shouldFailClosed() {
        VagwAuthBypassProperties props = accountModeProps();
        props.setServiceCertCn("");
        props.setServiceUsername("");
        props.setServicePassword("");
        assertFalse(newGuard(props).isBypassAllowed(certRequest()));
        verify(metrics).incBypassDenied("no_service_credential");
    }

    @Test
    void certMode_peerCertCnMismatch_shouldFailClosed() {
        VagwAuthBypassProperties props = certModeProps();
        MqttAuthRequest request = certRequest();
        request.setPeerCertCn("some-other-cn");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_cert_cn");
    }

    @Test
    void certMode_peerCertCnMissing_shouldFailClosed() {
        VagwAuthBypassProperties props = certModeProps();
        MqttAuthRequest request = certRequest();
        request.setPeerCertCn(null);
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_cert_cn");
    }

    @Test
    void certMode_caseInsensitiveMatch_shouldAllow() {
        // 沿用既有 equalsIgnoreCase 行为
        VagwAuthBypassProperties props = certModeProps();
        MqttAuthRequest request = certRequest();
        request.setPeerCertCn("Vagw-Service");
        assertTrue(newGuard(props).isBypassAllowed(request));
    }

    @Test
    void certMode_correctAccountCredsMustNotFallback() {
        // mTLS 模式优先：证书 CN 不匹配/缺失时，即使账号凭据正确也不得回退放行
        VagwAuthBypassProperties props = certModeProps();
        props.setServiceUsername("vagw-service");
        props.setServicePassword("s3cret-pass");
        MqttAuthRequest request = accountRequest("vagw-service", "s3cret-pass");
        request.setPeerCertCn(null);
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_cert_cn");
    }

    @Test
    void certMode_allFactorsSatisfied_shouldAllowBypass() {
        assertTrue(newGuard(certModeProps()).isBypassAllowed(certRequest()));
    }

    // ---------- 因子 2：明文账号模式（EDD-VAGW-DSN-CR-010）----------

    @Test
    void accountMode_allFactorsSatisfied_shouldAllowBypass() {
        assertTrue(newGuard(accountModeProps()).isBypassAllowed(accountRequest("vagw-service", "s3cret-pass")));
    }

    @Test
    void accountMode_wrongUsername_shouldFailClosed() {
        VagwAuthBypassProperties props = accountModeProps();
        MqttAuthRequest request = accountRequest("attacker", "s3cret-pass");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_username");
    }

    @Test
    void accountMode_wrongPassword_shouldFailClosed() {
        VagwAuthBypassProperties props = accountModeProps();
        MqttAuthRequest request = accountRequest("vagw-service", "wrong-pass");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_password");
    }

    @Test
    void accountMode_missingUsername_shouldFailClosed() {
        VagwAuthBypassProperties props = accountModeProps();
        MqttAuthRequest request = accountRequest(null, "s3cret-pass");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_username");
    }

    @Test
    void accountMode_missingPassword_shouldFailClosed() {
        VagwAuthBypassProperties props = accountModeProps();
        MqttAuthRequest request = accountRequest("vagw-service", null);
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("service_password");
    }

    // ---------- 因子 3：clientId ----------

    @Test
    void clientIdPrefixOnly_shouldFailClosed() {
        // 仅前缀、无随机后缀 —— 原实现的提权路径，必须拒绝
        VagwAuthBypassProperties props = accountModeProps();
        MqttAuthRequest request = accountRequest("vagw-service", "s3cret-pass");
        request.setClientId("vehicle-access-gateway");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("clientid");
    }

    @Test
    void fakeClientIdPrefixWithWrongFactors_shouldFailClosed() {
        // 伪造前缀 + peerhost 不在白名单 —— 四因子任一不满足即拒绝
        VagwAuthBypassProperties props = accountModeProps();
        MqttAuthRequest request = accountRequest("vagw-service", "s3cret-pass");
        request.setClientId("vehicle-access-gateway-evil");
        request.setPeerHost("203.0.113.9");
        assertFalse(newGuard(props).isBypassAllowed(request));
        verify(metrics).incBypassDenied("peerhost");
    }
}
