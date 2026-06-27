package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 设备绑定解析服务实现
 * 通过 VMD/TSP 解析 device_sn↔VIN 绑定关系，使用 Redis 缓存
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BindingServiceImpl implements BindingService {

    private static final String BINDING_KEY_PREFIX = "vagw:binding:";
    private static final long BINDING_CACHE_TTL_HOURS = 24;
    
    private final StringRedisTemplate redisTemplate;
    
    // TODO: Inject VMD/TSP client for actual binding resolution
    
    @Override
    public Optional<String> resolveVin(String deviceSn) {
        if (deviceSn == null || deviceSn.isBlank()) {
            return Optional.empty();
        }
        
        try {
            String key = BINDING_KEY_PREFIX + deviceSn;
            String cachedVin = redisTemplate.opsForValue().get(key);
            if (cachedVin != null) {
                log.debug("Binding cache hit: deviceSn={}, vin={}", deviceSn, cachedVin);
                return Optional.of(cachedVin);
            }
            
            // TODO: Call VMD/TSP to resolve binding
            log.debug("Binding not found for deviceSn={}", deviceSn);
            return Optional.empty();
            
        } catch (Exception e) {
            log.error("Failed to resolve binding for deviceSn={}", deviceSn, e);
            return Optional.empty();
        }
    }
    
    @Override
    public Optional<String> resolveDeviceSn(String vin) {
        if (vin == null || vin.isBlank()) {
            return Optional.empty();
        }
        
        try {
            // TODO: Implement reverse lookup if needed
            log.debug("Reverse binding lookup not implemented for vin={}", vin);
            return Optional.empty();
            
        } catch (Exception e) {
            log.error("Failed to resolve device_sn for vin={}", vin, e);
            return Optional.empty();
        }
    }
    
    @Override
    public boolean isValidAndBound(String deviceSn) {
        return resolveVin(deviceSn).isPresent();
    }
    
    /**
     * 缓存绑定关系
     */
    public void cacheBinding(String deviceSn, String vin) {
        try {
            String key = BINDING_KEY_PREFIX + deviceSn;
            redisTemplate.opsForValue().set(key, vin, BINDING_CACHE_TTL_HOURS, TimeUnit.HOURS);
            log.info("Cached binding: deviceSn={}, vin={}", deviceSn, vin);
        } catch (Exception e) {
            log.error("Failed to cache binding: deviceSn={}, vin={}", deviceSn, vin, e);
        }
    }
}
