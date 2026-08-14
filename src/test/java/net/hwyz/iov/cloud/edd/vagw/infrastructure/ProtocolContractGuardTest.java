package net.hwyz.iov.cloud.edd.vagw.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import vehicle.common.v1.Envelope;
import vagw.v1.Delivery;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 协议契约启动门禁测试（EDD-VAGW-DSN-CR-006 §5/§8.3）。
 */
class ProtocolContractGuardTest {

    private static ProtocolContractGuard guard(int protocolMajor) {
        ProtocolContractGuard g = new ProtocolContractGuard(new ObjectMapper(), protocolMajor);
        g.verify(); // 读取真实 manifest/registry/jar 资源
        return g;
    }

    @Test
    void verify_withPublishedArtifacts_shouldPass() {
        ProtocolContractGuard g = guard(1);
        assertTrue(g.payloadTypes().contains("vehicle.fota.v1.TaskCheckRequest"));
        assertFalse(g.payloadTypes().contains("vehicle.ota.v1.Task"));
        assertEquals(1, g.protocolMajor());
        assertEquals("0.0.1-SNAPSHOT", g.parProtoRelease());
    }

    @Test
    void verify_protocolMajorMismatch_shouldFailClosed() {
        assertThrows(IllegalStateException.class, () -> guard(2));
    }

    @Test
    void assertSha_drift_shouldFailClosed() {
        // 期望值与真实 DescriptorSet 不一致 → 启动失败
        assertThrows(IllegalStateException.class, () ->
                ProtocolContractGuard.assertSha("common", 
                        ProtocolContractGuard.descriptorSetBytes(Envelope.getDescriptor()),
                        "0000000000000000000000000000000000000000000000000000000000000000"));
    }

    @Test
    void descriptorSetBytes_hash_shouldMatchRecordedManifest() {
        // 复现 PAR-PROTO 记录的 common/vagw DescriptorSet sha（防字段号漂移）
        assertEquals("4ba4c1470270b16a44f30ce3bfff2062a703c0c88ac56bf89925a810651bc200",
                ProtocolContractGuard.sha256(
                        ProtocolContractGuard.descriptorSetBytes(Envelope.getDescriptor())));
        assertEquals("556e3a3d3916e65ce03e392440911d5d01ae899d61777cd8a7b8e84679c913d7",
                ProtocolContractGuard.sha256(
                        ProtocolContractGuard.descriptorSetBytes(Delivery.getDescriptor())));
    }
}
