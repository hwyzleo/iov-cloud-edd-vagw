package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import lombok.extern.slf4j.Slf4j;

/**
 * VMD relay 默认安全实现（EDD-VAGW-DSN-CR-011 §3.5，fail-closed）。
 * <p>
 * 在 VMD 正式 Service API（VMD-REQ-CR-055 / VMD-SPEC §5.2.10）契约发布并接入前，VAGW 不直连
 * KMS、也不猜测私有 wire；本实现始终返回不可用，keyprov 上行 fail-closed，等待车端用原
 * requestId 对账。后续由 VMD CR 提供真实 {@link VmdKeyProvisioningClient} Bean 覆盖。
 * </p>
 */
@Slf4j
public class DisabledVmdKeyProvisioningClient implements VmdKeyProvisioningClient {

    @Override
    public VmdRelayResponse relay(VmdRelayRequest request) {
        String requestId = request != null && request.envelope() != null
                ? request.envelope().getRequestId() : null;
        log.warn("VMD keyprov adapter not wired (disabled default); relay fail-closed, requestId={}", requestId);
        return VmdRelayResponse.unavailable("KEYPROV_VMD_UNAVAILABLE");
    }
}
