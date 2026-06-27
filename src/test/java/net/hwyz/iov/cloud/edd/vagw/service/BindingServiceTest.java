package net.hwyz.iov.cloud.edd.vagw.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BindingServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;
    
    @Mock
    private ValueOperations<String, String> valueOperations;
    
    @InjectMocks
    private BindingServiceImpl bindingService;

    @Test
    void resolveVin_cacheHit_shouldReturnVin() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:binding:DEVICE001")).thenReturn("LSGJA52U7YA000001");
        
        Optional<String> result = bindingService.resolveVin("DEVICE001");
        
        assertTrue(result.isPresent());
        assertEquals("LSGJA52U7YA000001", result.get());
    }

    @Test
    void resolveVin_cacheMiss_shouldReturnEmpty() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:binding:DEVICE001")).thenReturn(null);
        
        Optional<String> result = bindingService.resolveVin("DEVICE001");
        
        assertFalse(result.isPresent());
    }

    @Test
    void resolveVin_nullInput_shouldReturnEmpty() {
        Optional<String> result = bindingService.resolveVin(null);
        assertFalse(result.isPresent());
    }

    @Test
    void resolveVin_blankInput_shouldReturnEmpty() {
        Optional<String> result = bindingService.resolveVin("");
        assertFalse(result.isPresent());
    }

    @Test
    void cacheBinding_shouldCallRedis() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        
        bindingService.cacheBinding("DEVICE001", "LSGJA52U7YA000001");
        
        verify(valueOperations).set(eq("vagw:binding:DEVICE001"), eq("LSGJA52U7YA000001"), eq(24L), eq(TimeUnit.HOURS));
    }

    @Test
    void isValidAndBound_withBinding_shouldReturnTrue() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:binding:DEVICE001")).thenReturn("LSGJA52U7YA000001");
        
        assertTrue(bindingService.isValidAndBound("DEVICE001"));
    }

    @Test
    void isValidAndBound_withoutBinding_shouldReturnFalse() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("vagw:binding:DEVICE001")).thenReturn(null);
        
        assertFalse(bindingService.isValidAndBound("DEVICE001"));
    }
}
