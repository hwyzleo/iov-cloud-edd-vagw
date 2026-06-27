package net.hwyz.iov.cloud.edd.vagw.service;

import java.util.Optional;

/**
 * 设备绑定解析服务
 * 负责 device_sn↔VIN 的双向解析
 */
public interface BindingService {
    
    /**
     * 根据 device_sn 解析 VIN
     * @param deviceSn 设备序列号
     * @return VIN，如果未绑定则返回 empty
     */
    Optional<String> resolveVin(String deviceSn);
    
    /**
     * 根据 VIN 解析 device_sn
     * @param vin 车辆识别码
     * @return device_sn，如果未绑定则返回 empty
     */
    Optional<String> resolveDeviceSn(String vin);
    
    /**
     * 检查 device_sn 是否有效且已绑定
     * @param deviceSn 设备序列号
     * @return true 如果有效且已绑定
     */
    boolean isValidAndBound(String deviceSn);
}
