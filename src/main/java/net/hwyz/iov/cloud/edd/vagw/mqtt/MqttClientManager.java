package net.hwyz.iov.cloud.edd.vagw.mqtt;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttIngressAdapter;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.VehicleMessageUplinkBridge;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * VAGW MQTT 客户端与统一上行共享订阅管理（EDD-VAGW-DSN-CR-006 / CR-011 / CR-015）。
 * <p>
 * 所有上行订阅统一由 {@link VagwUplinkSubscriptionRegistry} 生成 {@code $share/vagw/{filter}}，
 * 不再硬编码已废弃的无组名兼容语法或按业务拆分的旧共享组。逐 filter 解析 SUBACK，失败只降级对应
 * capability，不取消其他 filter、不重建整个客户端；topic segment 分发到各自独立入口 Adapter。
 * </p>
 */
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

    @Autowired
    @Lazy
    private VehicleMessageUplinkBridge uplinkBridge;
    @Autowired
    private ObjectProvider<KeyprovMqttIngressAdapter> keyprovIngressAdapterProvider;
    @Autowired
    @Lazy
    private VagwUplinkSubscriptionRegistry subscriptionRegistry;
    @Autowired
    private VagwMqttSubscriptionMetrics metrics;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private MqttClient mqttClient;

    /** 已成功 SUBACK 的订阅，按 topic segment 索引；只用于消息分发与拒绝判定。 */
    private volatile Map<String, VagwUplinkSubscriptionRegistry.ResolvedSubscription> activeBySegment = Map.of();

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

        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                if (reconnect) {
                    // cleanSession=true 时自动重连不会恢复订阅；重连后按同一 registry 重新订阅
                    try {
                        subscribeActive();
                    } catch (Exception e) {
                        log.error("MQTT resubscribe after reconnect failed: {}", e.getMessage());
                    }
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                activeBySegment = Map.of();
                log.warn("MQTT connection lost: {}", cause == null ? null : cause.getMessage());
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
        subscribeActive();

        log.info("MQTT connected: broker={}, clientId={}", brokerUrl, fullClientId);
    }

    /**
     * 订阅当前生效的上行 filter（统一 {@code $share/vagw/...}）。逐 filter 解析 SUBACK，
     * 失败仅降级该 capability，并输出有界原因；不取消其他 filter。
     */
    void subscribeActive() throws MqttException {
        List<VagwUplinkSubscriptionRegistry.ResolvedSubscription> subscriptions =
                subscriptionRegistry.resolveActive();
        if (subscriptions.isEmpty()) {
            activeBySegment = Map.of();
            log.warn("MQTT uplink subscription set is empty; no subscription created");
            return;
        }

        String[] topicFilters = subscriptions.stream()
                .map(VagwUplinkSubscriptionRegistry.ResolvedSubscription::sharedTopic)
                .toArray(String[]::new);
        int[] qos = subscriptions.stream()
                .mapToInt(VagwUplinkSubscriptionRegistry.ResolvedSubscription::qos)
                .toArray();

        IMqttToken token;
        try {
            token = mqttClient.subscribeWithResponse(topicFilters, qos);
        } catch (MqttException e) {
            for (VagwUplinkSubscriptionRegistry.ResolvedSubscription subscription : subscriptions) {
                metrics.suback(subscription.group(), subscription.service(), "failure");
                metrics.subscriptionState(subscription.group(), subscription.service(), subscription.filter(), false);
            }
            activeBySegment = Map.of();
            log.error("MQTT subscribe request failed; all uplink capabilities degraded: {}", e.getMessage());
            return;
        }

        int[] granted = token == null ? null : token.getGrantedQos();
        Map<String, VagwUplinkSubscriptionRegistry.ResolvedSubscription> active = new HashMap<>();
        for (int i = 0; i < subscriptions.size(); i++) {
            VagwUplinkSubscriptionRegistry.ResolvedSubscription subscription = subscriptions.get(i);
            int grantedQos = (granted != null && i < granted.length) ? granted[i] : -1;
            if (grantedQos >= 0 && grantedQos != 0x80) {
                active.put(subscription.topicSegment(), subscription);
                metrics.suback(subscription.group(), subscription.service(), "success");
                metrics.subscriptionState(subscription.group(), subscription.service(), subscription.filter(), true);
                log.info("VAGW uplink subscription active: group={}, service={}, filter={}, qos={}",
                        subscription.group(), subscription.service(), subscription.filter(), grantedQos);
            } else {
                metrics.suback(subscription.group(), subscription.service(), "failure");
                metrics.subscriptionState(subscription.group(), subscription.service(), subscription.filter(), false);
                log.error("VAGW uplink subscription degraded (SUBACK failure): group={}, service={}, filter={}, reason=SUBACK_0x80",
                        subscription.group(), subscription.service(), subscription.filter());
            }
        }
        activeBySegment = Map.copyOf(active);
    }

    void handleMessage(String topic, MqttMessage message) {
        String[] parts = topic == null ? new String[0] : topic.split("/");
        if (parts.length < 4 || !"vehicle".equals(parts[0]) || !"up".equals(parts[2])) {
            metrics.uplinkRejected("unknown", "invalid_topic_format");
            log.warn("Invalid uplink topic format, rejected");
            return;
        }

        String segment = parts[3];
        VagwUplinkSubscriptionRegistry.ResolvedSubscription subscription = activeBySegment.get(segment);
        if (subscription == null) {
            metrics.uplinkRejected(segment, "filter_not_subscribed");
            log.warn("Uplink topic segment not subscribed, rejected: segment={}", segment);
            return;
        }

        metrics.uplinkReceived(subscription.group(), subscription.service());
        metrics.inflight(subscription.service(), 1);
        try {
            UplinkSubscriptionHandler handler = handlerForSegment(segment);
            if (handler == null) {
                metrics.uplinkRejected(subscription.service(), "no_handler");
                log.error("No uplink handler bound for segment={}", segment);
                return;
            }
            handler.handleUplink(topic, message.getPayload());
        } catch (Exception e) {
            metrics.uplinkRejected(subscription.service(), "handler_error");
            log.error("Error handling MQTT message: service={}", subscription.service(), e);
        } finally {
            metrics.inflight(subscription.service(), -1);
        }
    }

    private UplinkSubscriptionHandler handlerForSegment(String segment) {
        if (uplinkBridge.topicSegment().equals(segment)) {
            return uplinkBridge;
        }
        // keyprov 关闭时不创建该 Bean，避免功能关闭后依赖注入失败（EDD-VAGW-DSN-CR-016 §4）
        KeyprovMqttIngressAdapter keyprovIngressAdapter = keyprovIngressAdapterProvider.getIfAvailable();
        if (keyprovIngressAdapter != null && keyprovIngressAdapter.topicSegment().equals(segment)) {
            return keyprovIngressAdapter;
        }
        return null;
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
