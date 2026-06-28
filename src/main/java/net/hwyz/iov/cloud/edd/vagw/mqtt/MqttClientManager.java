package net.hwyz.iov.cloud.edd.vagw.mqtt;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.service.UplinkService;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class MqttClientManager {

    @Value("${spring.mqtt.url:tcp://localhost:1883}")
    private String brokerUrl;

    @Value("${spring.mqtt.client.id:vehicle-access-gateway}")
    private String clientId;

    @Value("${spring.mqtt.username:}")
    private String username;

    @Value("${spring.mqtt.password:}")
    private String password;

    @Value("${spring.mqtt.connect.delay-seconds:5}")
    private int connectDelaySeconds;

    private final UplinkService uplinkService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private MqttClient mqttClient;
    private volatile boolean running = false;

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        log.info("Application ready, scheduling MQTT connection in {} seconds...", connectDelaySeconds);
        scheduler.schedule(this::connectWithRetry, connectDelaySeconds, TimeUnit.SECONDS);
    }

    private void connectWithRetry() {
        int maxRetries = 5;
        int retryDelay = 5;

        for (int i = 0; i < maxRetries; i++) {
            try {
                connect();
                return; // 成功连接，退出
            } catch (Exception e) {
                log.warn("MQTT connection attempt {}/{} failed: {}", i + 1, maxRetries, e.getMessage());
                if (i < maxRetries - 1) {
                    try {
                        Thread.sleep(retryDelay * 1000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
        log.error("Failed to connect MQTT after {} attempts", maxRetries);
    }

    private void connect() throws MqttException {
        String fullClientId = clientId + java.util.UUID.randomUUID().toString().substring(0, 8);
        mqttClient = new MqttClient(brokerUrl, fullClientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setConnectionTimeout(60);
        options.setKeepAliveInterval(20);
        options.setAutomaticReconnect(true);
        options.setMaxReconnectDelay(10000);

        if (username != null && !username.isBlank()) {
            options.setUserName(username);
            options.setPassword(password != null ? password.toCharArray() : new char[0]);
        }

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                log.warn("MQTT connection lost: {}", cause.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                handleMessage(topic, message);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                // Not used for uplink
            }
        });

        mqttClient.connect(options);
        running = true;

        // Subscribe to uplink shared subscription
        String[] topics = {"$queue/vehicle/+/up/#"};
        int[] qos = {1};
        mqttClient.subscribe(topics, qos);

        log.info("MQTT connected and subscribed: broker={}, clientId={}", brokerUrl, fullClientId);
    }

    private void handleMessage(String topic, MqttMessage message) {
        try {
            // Extract device_sn from topic: vehicle/{device_sn}/up/{service}
            String[] parts = topic.split("/");
            if (parts.length < 4 || !"vehicle".equals(parts[0]) || !"up".equals(parts[2])) {
                log.warn("Invalid uplink topic format: {}", topic);
                return;
            }

            String deviceSn = parts[1];
            String service = parts[3];

            log.debug("Uplink message: topic={}, deviceSn={}, service={}, payloadSize={}",
                    topic, deviceSn, service, message.getPayload().length);

            UplinkService.ProcessResult result = uplinkService.processUplink(
                    message.getPayload(), deviceSn);

            if (!result.ok()) {
                log.warn("Uplink processing failed: deviceSn={}, reason={}", deviceSn, result.reason());
            }
        } catch (Exception e) {
            log.error("Error handling MQTT message: topic={}", topic, e);
        }
    }

    public void publish(String topic, byte[] payload, int qos) throws MqttException {
        if (mqttClient != null && mqttClient.isConnected()) {
            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(qos);
            mqttClient.publish(topic, msg);
        } else {
            throw new MqttException(MqttException.REASON_CODE_CLIENT_EXCEPTION);
        }
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        scheduler.shutdown();
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                mqttClient.disconnect();
                mqttClient.close();
                log.info("MQTT client disconnected");
            } catch (MqttException e) {
                log.error("Error disconnecting MQTT client", e);
            }
        }
    }
}
