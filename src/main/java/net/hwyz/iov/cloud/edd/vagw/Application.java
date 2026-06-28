package net.hwyz.iov.cloud.edd.vagw;

import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.framework.security.annotation.EnableCustomConfig;
import net.hwyz.iov.cloud.framework.security.annotation.EnableCustomFeignClients;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@EnableCustomConfig
@EnableDiscoveryClient
@SpringBootApplication
@EnableCustomFeignClients
public class Application {

    static {
        System.setProperty("nacos.logging.default.config.enabled", "false");
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        log.info("VAGW started on port {}", System.getProperty("server.port", "10804"));
    }
}
