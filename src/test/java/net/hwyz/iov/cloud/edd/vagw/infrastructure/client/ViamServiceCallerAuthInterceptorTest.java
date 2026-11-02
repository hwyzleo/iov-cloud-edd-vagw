package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.RequestTemplate;
import feign.Target;
import net.hwyz.iov.cloud.edd.vagw.config.VagwViamSecurityProperties;
import net.hwyz.iov.cloud.framework.common.constant.ServiceNameConstants;
import net.hwyz.iov.cloud.sec.viam.api.security.ViamServiceAuthHeaders;
import net.hwyz.iov.cloud.sec.viam.api.security.ViamServiceSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * VAGW → VIAM 服务间签名拦截器测试（SEC-VIAM-DSN-CR-001 / G-4，EDD-VAGW-DSN-CR-019 配套）。
 * <p>
 * 验证对官方 {@code sec-viam} 的 Feign 调用附加完整 {@code X-Viam-*} 签名头、签名与官方
 * {@link ViamServiceSignature} 验签端可复核一致、仅作用于 VIAM 服务（不影响其它客户端）、
 * GET 空 body 摘要与 POST body 摘要正确。
 * </p>
 */
class ViamServiceCallerAuthInterceptorTest {

    private static final byte[] SECRET = "test-secret-bytes-for-hmac".getBytes(StandardCharsets.UTF_8);
    private static final String SECRET_B64 = Base64.getEncoder().encodeToString(SECRET);

    private VagwViamSecurityProperties properties;
    private ViamServiceCallerAuthInterceptor interceptor;

    @BeforeEach
    void setUp() {
        properties = new VagwViamSecurityProperties();
        properties.setEnabled(true);
        properties.setClientId("VAGW");
        properties.setKeyId("k1");
        properties.setSecret(SECRET_B64);
        properties.setAudience("sec-viam");
        properties.validate();
        interceptor = new ViamServiceCallerAuthInterceptor(properties);
    }

    private RequestTemplate template(String targetName, String method, String path, byte[] body) {
        RequestTemplate template = new RequestTemplate();
        template.method(method);
        template.uri(path);
        if (body != null) {
            template.body(body, StandardCharsets.UTF_8);
        }
        Target<?> target = mock(Target.class);
        when(target.name()).thenReturn(targetName);
        template.feignTarget(target);
        return template;
    }

    @Test
    void apply_viamTarget_shouldAttachAllSignatureHeaders() {
        byte[] body = "{\"uid\":\"DEVICE\"}".getBytes(StandardCharsets.UTF_8);
        RequestTemplate template = template(ServiceNameConstants.SEC_VIAM, "POST",
                "/api/service/deviceAdmission/v1/decide", body);

        interceptor.apply(template);

        Map<String, ?> headers = template.headers();
        assertEquals("VAGW", header(headers, ViamServiceAuthHeaders.CLIENT_ID));
        assertEquals("k1", header(headers, ViamServiceAuthHeaders.KEY_ID));
        assertEquals("sec-viam", header(headers, ViamServiceAuthHeaders.AUDIENCE));
        assertEquals("post", header(headers, ViamServiceAuthHeaders.METHOD));
        assertEquals("/api/service/deviceAdmission/v1/decide", header(headers, ViamServiceAuthHeaders.PATH));
        assertEquals(sha256Hex(body), header(headers, ViamServiceAuthHeaders.BODY_DIGEST));
        assertEquals("v1", header(headers, ViamServiceAuthHeaders.ALGORITHM));
        assertNotNull(header(headers, ViamServiceAuthHeaders.TIMESTAMP));
        assertNotNull(header(headers, ViamServiceAuthHeaders.NONCE));
        assertNotNull(header(headers, ViamServiceAuthHeaders.SIGNATURE));
    }

    @Test
    void apply_viamTarget_shouldProduceSignatureReplicableByOfficialPrimitive() {
        byte[] body = "{\"requestId\":\"r1\"}".getBytes(StandardCharsets.UTF_8);
        RequestTemplate template = template(ServiceNameConstants.SEC_VIAM, "POST",
                "/api/service/keyProvisioning/v1/request", body);

        interceptor.apply(template);

        String clientId = (String) header(template.headers(), ViamServiceAuthHeaders.CLIENT_ID);
        String keyId = (String) header(template.headers(), ViamServiceAuthHeaders.KEY_ID);
        String audience = (String) header(template.headers(), ViamServiceAuthHeaders.AUDIENCE);
        String method = (String) header(template.headers(), ViamServiceAuthHeaders.METHOD);
        String path = (String) header(template.headers(), ViamServiceAuthHeaders.PATH);
        String bodyDigest = (String) header(template.headers(), ViamServiceAuthHeaders.BODY_DIGEST);
        String timestamp = (String) header(template.headers(), ViamServiceAuthHeaders.TIMESTAMP);
        String nonce = (String) header(template.headers(), ViamServiceAuthHeaders.NONCE);
        String signature = (String) header(template.headers(), ViamServiceAuthHeaders.SIGNATURE);

        // 用官方验签原语重算签名：必须与拦截器输出一致（VIAM 端可复核）
        String canonical = ViamServiceSignature.canonicalString(
                clientId, keyId, audience, method, path, bodyDigest, timestamp, nonce);
        String expected = ViamServiceSignature.sign(SECRET, canonical);
        assertTrue(ViamServiceSignature.constantTimeEquals(expected, signature),
                "签名必须与官方 ViamServiceSignature 验签原语一致");
    }

    @Test
    void apply_getRequest_shouldUseEmptyBodyDigest() {
        RequestTemplate template = template(ServiceNameConstants.SEC_VIAM, "GET",
                "/api/service/vehicleAccessIdentity/v1/byVin/VIN1", null);

        interceptor.apply(template);

        assertEquals(sha256Hex(new byte[0]), header(template.headers(), ViamServiceAuthHeaders.BODY_DIGEST));
        assertEquals("get", header(template.headers(), ViamServiceAuthHeaders.METHOD));
    }

    @Test
    void apply_nonViamTarget_shouldNotAttachHeaders() {
        RequestTemplate template = template("other-service", "POST", "/api/other", new byte[]{1});

        interceptor.apply(template);

        assertNull(header(template.headers(), ViamServiceAuthHeaders.CLIENT_ID));
        assertNull(header(template.headers(), ViamServiceAuthHeaders.SIGNATURE));
    }

    /** Feign RequestTemplate 头为多值 Collection，取首个值。 */
    private static String header(Map<String, ?> headers, String name) {
        Object value = headers.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Collection<?> collection && !collection.isEmpty()) {
            return collection.iterator().next().toString();
        }
        return value.toString();
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
