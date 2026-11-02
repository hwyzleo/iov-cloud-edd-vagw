package net.hwyz.iov.cloud.edd.vagw.config;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * VAGW → VIAM 签名配置校验测试（SEC-VIAM-DSN-CR-001 / G-4，EDD-VAGW-DSN-CR-019 配套）。
 * <p>
 * enabled=true 时 Secret 缺失/非法 Base64/空解码必须 fail-fast，绝不静默降级为未认证调用。
 * </p>
 */
class VagwViamSecurityPropertiesTest {

    @Test
    void enabledWithValidSecret_shouldPass() {
        VagwViamSecurityProperties properties = new VagwViamSecurityProperties();
        properties.setEnabled(true);
        properties.setClientId("VAGW");
        properties.setKeyId("k1");
        properties.setSecret(Base64.getEncoder().encodeToString("secret".getBytes()));
        properties.setAudience("sec-viam");

        properties.validate();

        assertArrayEquals("secret".getBytes(), properties.secretBytes());
    }

    @Test
    void enabledWithMissingSecret_shouldFailFast() {
        VagwViamSecurityProperties properties = new VagwViamSecurityProperties();
        properties.setEnabled(true);
        properties.setClientId("VAGW");
        properties.setKeyId("k1");
        properties.setSecret("");
        properties.setAudience("sec-viam");

        assertThrows(IllegalStateException.class, properties::validate);
    }

    @Test
    void enabledWithInvalidBase64Secret_shouldFailFast() {
        VagwViamSecurityProperties properties = new VagwViamSecurityProperties();
        properties.setEnabled(true);
        properties.setClientId("VAGW");
        properties.setKeyId("k1");
        properties.setSecret("not-valid-base64!!!");
        properties.setAudience("sec-viam");

        assertThrows(IllegalStateException.class, properties::validate);
    }

    @Test
    void enabledWithMissingClientIdOrKeyId_shouldFailFast() {
        VagwViamSecurityProperties properties = new VagwViamSecurityProperties();
        properties.setEnabled(true);
        properties.setClientId("");
        properties.setKeyId("k1");
        properties.setSecret(Base64.getEncoder().encodeToString("secret".getBytes()));
        properties.setAudience("sec-viam");

        assertThrows(IllegalStateException.class, properties::validate);
    }

    @Test
    void disabled_shouldNotRequireSecret() {
        VagwViamSecurityProperties properties = new VagwViamSecurityProperties();
        properties.setEnabled(false);
        properties.setSecret("");

        properties.validate(); // 不抛异常
    }
}
