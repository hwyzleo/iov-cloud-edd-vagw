package net.hwyz.iov.cloud.edd.vagw.config;

import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.DisabledVmdKeyProvisioningClient;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.client.VmdKeyProvisioningClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * keyprov VMD SPI 装配（EDD-VAGW-DSN-CR-011 §3.5）。
 * <p>
 * 默认注入 {@link DisabledVmdKeyProvisioningClient}（fail-closed）；待 VMD 正式 CR 提供真实
 * {@link VmdKeyProvisioningClient} Bean 时自动覆盖，无需改动 VAGW relay 编排。
 * </p>
 */
@Configuration
public class KeyprovClientConfig {

    @Bean
    @ConditionalOnMissingBean(VmdKeyProvisioningClient.class)
    public VmdKeyProvisioningClient disabledVmdKeyProvisioningClient() {
        return new DisabledVmdKeyProvisioningClient();
    }
}
