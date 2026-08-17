package net.hwyz.iov.cloud.edd.vagw.infrastructure.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.infrastructure.LogMask;
import net.hwyz.iov.cloud.iov.tsp.api.service.TspVehicleAccessIdentityService;
import net.hwyz.iov.cloud.iov.tsp.api.vo.VehicleAccessIdentityResultVo;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * TSP 车辆接入身份反查客户端（EDD-VAGW-DSN-CR-007 §3）。
 * <p>
 * 封装 TSP Feign 契约 {@link TspVehicleAccessIdentityService#resolveByVin(String)}：
 * 校验响应结构与必填字段，将 ResolveStatus/ResolveReason 映射为 VAGW 内部结果；
 * 超时、连接失败、熔断或非法响应统一映射 DEPENDENCY_UNAVAILABLE。
 * fail-closed：不返回伪造 HSM UID，不从本地配置或历史字段兜底。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TspVehicleAccessIdentityClient {

    /** TSP 语义上的业务未绑定 reason（映射为 VAGW UNBOUND，外部错误码 804008） */
    private static final Set<String> UNBOUND_REASONS =
            Set.of("VIN_UNKNOWN", "UNBOUND", "TBOX_NOT_FOUND", "HSM_UID_MISSING");

    private final TspVehicleAccessIdentityService tspVehicleAccessIdentityService;

    /**
     * 按 VIN 解析当前接入身份（hsmUid）。
     *
     * @param vin 车辆 VIN
     * @return TSP 解析结果（fail-closed）
     */
    public TspResolveResult resolveByVin(String vin) {
        if (vin == null || vin.isBlank()) {
            return TspResolveResult.unresolved(vin, "INVALID_ARGUMENT");
        }
        try {
            VehicleAccessIdentityResultVo vo = tspVehicleAccessIdentityService.resolveByVin(vin);
            return map(vo);
        } catch (Exception e) {
            // 超时 / 连接失败 / 熔断异常 → fail-closed
            log.error("TSP resolveByVin failed: vin={}, err={}", LogMask.mask(vin), e.getMessage());
            return TspResolveResult.unresolved(vin, "DEPENDENCY_UNAVAILABLE");
        }
    }

    private TspResolveResult map(VehicleAccessIdentityResultVo vo) {
        if (vo == null) {
            return TspResolveResult.unresolved(null, "DEPENDENCY_UNAVAILABLE");
        }
        String status = vo.getStatus();
        if ("RESOLVED".equalsIgnoreCase(status)) {
            // 校验必填字段，缺失则 fail-closed，禁止以空身份继续
            if (isBlank(vo.getHsmUid()) || isBlank(vo.getTboxSn()) || vo.getBindingVersion() == null) {
                log.warn("TSP resolveByVin invalid resolved response (missing required field): vin={}",
                        LogMask.mask(vo.getVin()));
                return TspResolveResult.unresolved(vo.getVin(), "DEPENDENCY_UNAVAILABLE");
            }
            return TspResolveResult.resolved(vo.getVin(), vo.getHsmUid(), vo.getTboxSn(),
                    vo.getBindingVersion(), vo.getBindingUpdatedAt());
        }

        // UNRESOLVED：按 reason 映射，未知 reason 一律 fail-closed
        String reason = vo.getReason();
        if ("BINDING_CONFLICT".equals(reason)) {
            return TspResolveResult.unresolved(vo.getVin(), "BINDING_CONFLICT");
        }
        if ("DEPENDENCY_UNAVAILABLE".equals(reason)) {
            return TspResolveResult.unresolved(vo.getVin(), "DEPENDENCY_UNAVAILABLE");
        }
        if (UNBOUND_REASONS.contains(reason)) {
            return TspResolveResult.unresolved(vo.getVin(), reason);
        }
        log.warn("TSP resolveByVin unknown reason, fail-closed: vin={}, reason={}",
                LogMask.mask(vo.getVin()), reason);
        return TspResolveResult.unresolved(vo.getVin(), "DEPENDENCY_UNAVAILABLE");
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
