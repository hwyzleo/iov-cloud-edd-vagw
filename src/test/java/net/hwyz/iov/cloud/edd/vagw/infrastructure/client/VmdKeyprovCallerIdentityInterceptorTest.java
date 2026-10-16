package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import feign.RequestTemplate;
import feign.Target;
import net.hwyz.iov.cloud.framework.common.constant.CustomHeaders;
import net.hwyz.iov.cloud.framework.common.constant.ServiceNameConstants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link VmdKeyprovCallerIdentityInterceptor} 测试（VMD-DSN-CR-059 §4/§6）。
 * <p>
 * 验证仅对官方 VMD 服务调用附加 {@code X-Client-Id=VAGW}，避免影响其他服务间调用。
 * </p>
 */
class VmdKeyprovCallerIdentityInterceptorTest {

    private final VmdKeyprovCallerIdentityInterceptor interceptor =
            new VmdKeyprovCallerIdentityInterceptor();

    @Test
    void vmdCall_shouldCarryVagwCallerIdentity() {
        RequestTemplate template = new RequestTemplate();
        template.feignTarget(target(ServiceNameConstants.EDD_VMD));

        interceptor.apply(template);

        assertEquals(VmdKeyprovCallerIdentityInterceptor.VAGW_CALLER_IDENTITY,
                template.headers().get(CustomHeaders.CLIENT_ID).iterator().next());
    }

    @Test
    void nonVmdCall_shouldNotCarryCallerIdentity() {
        RequestTemplate template = new RequestTemplate();
        template.feignTarget(target("iov-tsp"));

        interceptor.apply(template);

        assertFalse(template.headers().containsKey(CustomHeaders.CLIENT_ID));
    }

    @Test
    void missingTarget_shouldNotFailNorCarryIdentity() {
        RequestTemplate template = new RequestTemplate();

        interceptor.apply(template);

        assertFalse(template.headers().containsKey(CustomHeaders.CLIENT_ID));
    }

    @SuppressWarnings("unchecked")
    private static Target<Object> target(String name) {
        Target<Object> target = mock(Target.class);
        when(target.name()).thenReturn(name);
        return target;
    }
}
