package net.hwyz.iov.cloud.edd.vagw.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 绑定解析配置启动校验测试（EDD-VAGW-DSN-CR-020 §6）。
 */
@DisplayName("绑定解析配置启动校验")
class VagwBindingPropertiesTest {

    @Test
    @DisplayName("默认值合法：cache=300 / sessionMax=1800 / deadline=500 / concurrent=8")
    void defaults_shouldPassValidation() {
        VagwBindingProperties props = new VagwBindingProperties();

        assertDoesNotThrow(props::validate);
    }

    @Test
    @DisplayName("session-max 小于 cache-ttl：启动失败")
    void sessionMaxBelowCacheTtl_shouldFail() {
        VagwBindingProperties props = new VagwBindingProperties();
        props.setCacheTtlSeconds(300);
        props.setSessionMaxTtlSeconds(120);

        IllegalStateException e = assertThrows(IllegalStateException.class, props::validate);
        assertTrue(e.getMessage().contains("session-max-ttl-seconds"));
    }

    @Test
    @DisplayName("session-max 超出会话记录 TTL：启动失败")
    void sessionMaxAboveSessionRecordTtl_shouldFail() {
        VagwBindingProperties props = new VagwBindingProperties();
        props.setSessionMaxTtlSeconds(VagwBindingProperties.SESSION_RECORD_TTL_SECONDS + 1);

        assertThrows(IllegalStateException.class, props::validate);
    }

    @Test
    @DisplayName("cache-ttl 非正数：启动失败")
    void cacheTtlNonPositive_shouldFail() {
        VagwBindingProperties props = new VagwBindingProperties();
        props.setCacheTtlSeconds(0);

        assertThrows(IllegalStateException.class, props::validate);
    }

    @Test
    @DisplayName("回源 deadline / 并发上限越界：启动失败")
    void uplinkResolveBounds_shouldFail() {
        VagwBindingProperties tooSmallDeadline = new VagwBindingProperties();
        tooSmallDeadline.getUplinkResolve().setDeadlineMs(10);
        assertThrows(IllegalStateException.class, tooSmallDeadline::validate);

        VagwBindingProperties tooBigDeadline = new VagwBindingProperties();
        tooBigDeadline.getUplinkResolve().setDeadlineMs(5000);
        assertThrows(IllegalStateException.class, tooBigDeadline::validate);

        VagwBindingProperties noPermit = new VagwBindingProperties();
        noPermit.getUplinkResolve().setMaxConcurrent(0);
        assertThrows(IllegalStateException.class, noPermit::validate);

        VagwBindingProperties tooManyPermits = new VagwBindingProperties();
        tooManyPermits.getUplinkResolve().setMaxConcurrent(65);
        assertThrows(IllegalStateException.class, tooManyPermits::validate);
    }

    @Test
    @DisplayName("非生产排障关闭上行回源：允许启动（仅告警）")
    void uplinkResolveDisabled_shouldStillStart() {
        VagwBindingProperties props = new VagwBindingProperties();
        props.getUplinkResolve().setEnabled(false);

        assertDoesNotThrow(props::validate);
    }
}
