package net.hwyz.iov.cloud.edd.vagw.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {
    DEVICE_UNKNOWN(804001, "设备未知 / VIN 不存在"),
    DEVICE_BLOCKED(804002, "设备停用 / 黑名单 / 证书吊销"),
    VIN_MISMATCH(804003, "身份与报文 VIN 不一致"),
    INVALID_ENVELOPE(804004, "报文头非法 / 解析失败"),
    PAYLOAD_DECODE_FAILED(804005, "payload 解码失败"),
    ROUTE_UNAVAILABLE(804006, "路由目标不可用"),
    VEHICLE_OFFLINE(804007, "下行目标车辆离线"),
    VIN_UNAUTHORIZED(804008, "下行目标 VIN 未知 / 无权限"),
    ACL_SYNC_FAILED(804009, "ACL 下发 / 同步失败");

    private final int code;
    private final String message;
}
