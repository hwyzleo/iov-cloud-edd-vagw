package net.hwyz.iov.cloud.edd.vagw;


import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * 启动类
 *
 * @author hwyz_leo
 */
@Slf4j
@EnableDiscoveryClient
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        System.setProperty("nacos.logging.default.config.enabled","false");
        SpringApplication.run(Application.class, args);
        logger.info("应用启动完成");
    }

}
