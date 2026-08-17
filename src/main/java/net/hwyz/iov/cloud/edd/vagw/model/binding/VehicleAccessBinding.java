package net.hwyz.iov.cloud.edd.vagw.model.binding;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

/**
 * 车辆接入绑定（EDD-VAGW-DSN-CR-007 §2）。
 * <p>
 * VIN ↔ hsmUid（证书 CN / MQTT Topic device key / Envelope device_id 的规范值）双向绑定，
 * 携带 bindingVersion 用于换件/解绑治理。tboxSn 仅用于资产追溯，不得作为 MQTT device key。
 * 缓存项必须携带 bindingVersion 与更新时间，受 TTL 有界治理。
 * </p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VehicleAccessBinding implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 车辆 VIN */
    private String vin;
    /** HSM UID（南向接入身份规范值） */
    private String hsmUid;
    /** TBOX 序列号（仅资产追溯，不得作为接入身份） */
    private String tboxSn;
    /** 绑定版本 */
    private long bindingVersion;
    /** 绑定最近更新时间 */
    private Instant bindingUpdatedAt;
    /** 绑定来源 */
    private BindingSource source;
}
