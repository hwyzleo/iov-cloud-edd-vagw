package net.hwyz.iov.cloud.edd.vagw.config;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * VagwAdmissionProperties 启动校验测试（EDD-VAGW-DSN-CR-014 §8）：
 * {@code accepted-encodings} 必须恰好为 {PEM, BASE64_DER}；缺失、未知值或重复配置均启动失败。
 * 该校验不控制解析分支，运行时始终同时启用两种格式。
 */
class VagwAdmissionPropertiesTest {

    @Test
    void defaultAcceptedEncodings_shouldPass() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        assertDoesNotThrow(properties::validateAcceptedEncodings);
    }

    @Test
    void explicitAcceptedEncodings_shouldPass() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(Arrays.asList("PEM", "BASE64_DER"));
        assertDoesNotThrow(properties::validateAcceptedEncodings);
    }

    @Test
    void caseInsensitiveAcceptedEncodings_shouldPass() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(Arrays.asList("pem", "base64_der"));
        assertDoesNotThrow(properties::validateAcceptedEncodings);
    }

    @Test
    void nullAcceptedEncodings_shouldFailFast() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(null);
        assertThrows(IllegalStateException.class, properties::validateAcceptedEncodings);
    }

    @Test
    void emptyAcceptedEncodings_shouldFailFast() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(new ArrayList<>());
        assertThrows(IllegalStateException.class, properties::validateAcceptedEncodings);
    }

    @Test
    void missingEncoding_shouldFailFast() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(List.of("PEM"));
        assertThrows(IllegalStateException.class, properties::validateAcceptedEncodings);
    }

    @Test
    void unknownEncoding_shouldFailFast() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(Arrays.asList("PEM", "PKCS12"));
        assertThrows(IllegalStateException.class, properties::validateAcceptedEncodings);
    }

    @Test
    void duplicateEncoding_shouldFailFast() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(Arrays.asList("PEM", "PEM"));
        assertThrows(IllegalStateException.class, properties::validateAcceptedEncodings);
    }

    @Test
    void blankEncoding_shouldFailFast() {
        VagwAdmissionProperties properties = new VagwAdmissionProperties();
        properties.getPeerCertificate().setAcceptedEncodings(Arrays.asList("PEM", "  "));
        assertThrows(IllegalStateException.class, properties::validateAcceptedEncodings);
    }
}
