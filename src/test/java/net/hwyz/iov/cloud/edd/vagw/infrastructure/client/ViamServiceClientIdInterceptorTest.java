package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.RequestTemplate;
import feign.Target;
import net.hwyz.iov.cloud.framework.common.constant.CustomHeaders;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * VAGW → VIAM 服务身份注入测试（SEC-VIAM-DSN-CR-001 / R1）
 * <p>
 * 仅 SEC_VIAM Feign 作用域注入 X-Client-Id；其它服务不注入；无外部 HTTP 请求
 * （Kafka 触发 receipt）同样注入；外部同名头被覆盖。
 * </p>
 *
 * @author hwyz_leo
 */
class ViamServiceClientIdInterceptorTest {

    private RequestTemplate templateFor(String targetName) {
        RequestTemplate template = new RequestTemplate();
        Target<?> target = new Target.HardCodedTarget<>(
                net.hwyz.iov.cloud.sec.viam.api.service.ViamDeviceAdmissionService.class,
                targetName, "http://localhost/" + targetName);
        return template.feignTarget(target);
    }

    private ViamServiceClientIdInterceptor interceptor(String clientId) throws Exception {
        ViamServiceClientIdInterceptor interceptor = new ViamServiceClientIdInterceptor();
        Field field = ViamServiceClientIdInterceptor.class.getDeclaredField("clientId");
        field.setAccessible(true);
        field.set(interceptor, clientId);
        return interceptor;
    }

    @Test
    void injectsClientIdOnlyForSecViam() throws Exception {
        RequestTemplate viam = templateFor("sec-viam");
        interceptor("VAGW").apply(viam);
        assertEquals("VAGW", viam.headers().get(CustomHeaders.CLIENT_ID).iterator().next());

        // 非 VIAM 服务不注入
        RequestTemplate other = templateFor("other-service");
        interceptor("VAGW").apply(other);
        assertNull(other.headers().get(CustomHeaders.CLIENT_ID));
    }

    @Test
    void overridesExternalSameNameHeader() throws Exception {
        RequestTemplate viam = templateFor("sec-viam");
        viam.header(CustomHeaders.CLIENT_ID, "EXTERNAL");
        interceptor("VAGW").apply(viam);
        assertEquals("VAGW", viam.headers().get(CustomHeaders.CLIENT_ID).iterator().next());
    }

    @Test
    void blankConfigDoesNotInject() throws Exception {
        RequestTemplate viam = templateFor("sec-viam");
        interceptor("  ").apply(viam);
        assertTrue(viam.headers().get(CustomHeaders.CLIENT_ID) == null);
    }
}
