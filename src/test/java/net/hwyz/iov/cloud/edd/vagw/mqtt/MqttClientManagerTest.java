package net.hwyz.iov.cloud.edd.vagw.mqtt;

import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.KeyprovMqttIngressAdapter;
import net.hwyz.iov.cloud.edd.vagw.adapter.mqtt.VehicleMessageUplinkBridge;
import net.hwyz.iov.cloud.edd.vagw.service.VagwMqttSubscriptionMetrics;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * MQTT 客户端统一共享订阅与分发测试（EDD-VAGW-DSN-CR-015 §3/§4/§7）。
 */
class MqttClientManagerTest {

    private MqttClient mqttClient;
    private VagwUplinkSubscriptionRegistry subscriptionRegistry;
    private VehicleMessageUplinkBridge uplinkBridge;
    private KeyprovMqttIngressAdapter keyprovIngressAdapter;
    private VagwMqttSubscriptionMetrics metrics;
    private MqttClientManager manager;

    private static final VagwUplinkSubscriptionRegistry.ResolvedSubscription FOTA =
            new VagwUplinkSubscriptionRegistry.ResolvedSubscription(
                    "vagw", "fota", "fota", "vehicle/+/up/fota", "$share/vagw/vehicle/+/up/fota", 1);
    private static final VagwUplinkSubscriptionRegistry.ResolvedSubscription KEYPROV =
            new VagwUplinkSubscriptionRegistry.ResolvedSubscription(
                    "vagw", "security.keyprov", "keyprov", "vehicle/+/up/keyprov",
                    "$share/vagw/vehicle/+/up/keyprov", 1);

    @BeforeEach
    void setUp() {
        mqttClient = mock(MqttClient.class);
        subscriptionRegistry = mock(VagwUplinkSubscriptionRegistry.class);
        uplinkBridge = mock(VehicleMessageUplinkBridge.class);
        keyprovIngressAdapter = mock(KeyprovMqttIngressAdapter.class);
        metrics = new VagwMqttSubscriptionMetrics();

        @SuppressWarnings("unchecked")
        ObjectProvider<KeyprovMqttIngressAdapter> keyprovProvider = mock(ObjectProvider.class);
        when(keyprovProvider.getIfAvailable()).thenReturn(keyprovIngressAdapter);

        manager = new MqttClientManager();
        ReflectionTestUtils.setField(manager, "mqttClient", mqttClient);
        ReflectionTestUtils.setField(manager, "subscriptionRegistry", subscriptionRegistry);
        ReflectionTestUtils.setField(manager, "uplinkBridge", uplinkBridge);
        ReflectionTestUtils.setField(manager, "keyprovIngressAdapterProvider", keyprovProvider);
        ReflectionTestUtils.setField(manager, "metrics", metrics);
    }

    private IMqttToken tokenWith(int[] grantedQos) {
        IMqttToken token = mock(IMqttToken.class);
        when(token.getGrantedQos()).thenReturn(grantedQos);
        return token;
    }

    @Test
    void subscribeActive_shouldUseUnifiedSharedGroupTopicsAndQos() throws MqttException {
        when(subscriptionRegistry.resolveActive()).thenReturn(List.of(FOTA, KEYPROV));
        IMqttToken token = tokenWith(new int[]{1, 1});
        when(mqttClient.subscribeWithResponse(any(String[].class), any(int[].class)))
                .thenReturn(token);

        manager.subscribeActive();

        ArgumentCaptor<String[]> topics = ArgumentCaptor.forClass(String[].class);
        ArgumentCaptor<int[]> qos = ArgumentCaptor.forClass(int[].class);
        verify(mqttClient).subscribeWithResponse(topics.capture(), qos.capture());

        assertArrayEquals(new String[]{
                "$share/vagw/vehicle/+/up/fota",
                "$share/vagw/vehicle/+/up/keyprov"}, topics.getValue());
        assertArrayEquals(new int[]{1, 1}, qos.getValue());
        assertEquals(1L, metrics.gaugeValue(
                "vagw_mqtt_subscription_state{group=vagw,service=fota,filter=vehicle/+/up/fota}"));
        assertEquals(1L, metrics.gaugeValue(
                "vagw_mqtt_subscription_state{group=vagw,service=security.keyprov,filter=vehicle/+/up/keyprov}"));
    }

    @Test
    void handleMessage_shouldPrioritizeHandlerAndRouteBySegment() throws MqttException {
        when(subscriptionRegistry.resolveActive()).thenReturn(List.of(FOTA, KEYPROV));
        IMqttToken token = tokenWith(new int[]{1, 1});
        when(mqttClient.subscribeWithResponse(any(String[].class), any(int[].class)))
                .thenReturn(token);
        when(uplinkBridge.topicSegment()).thenReturn("fota");
        when(keyprovIngressAdapter.topicSegment()).thenReturn("keyprov");
        manager.subscribeActive();

        byte[] payload = new byte[]{1, 2, 3};
        manager.handleMessage("vehicle/DEVICE001/up/fota", new MqttMessage(payload));
        manager.handleMessage("vehicle/DEVICE001/up/keyprov", new MqttMessage(payload));

        verify(uplinkBridge).handleUplink("vehicle/DEVICE001/up/fota", payload);
        verify(keyprovIngressAdapter).handleUplink("vehicle/DEVICE001/up/keyprov", payload);
        assertEquals(1L, metrics.get("vagw_mqtt_uplink_received_total{group=vagw,service=fota}"));
        assertEquals(1L, metrics.get("vagw_mqtt_uplink_received_total{group=vagw,service=security.keyprov}"));
        assertEquals(0L, metrics.gaugeValue("vagw_mqtt_inflight{service=fota}"));
    }

    @Test
    void subscribeActive_partialSubackFailure_shouldDegradeOnlyFailedCapability() throws MqttException {
        when(subscriptionRegistry.resolveActive()).thenReturn(List.of(FOTA, KEYPROV));
        IMqttToken token = tokenWith(new int[]{1, 0x80});
        when(mqttClient.subscribeWithResponse(any(String[].class), any(int[].class)))
                .thenReturn(token);
        when(uplinkBridge.topicSegment()).thenReturn("fota");
        manager.subscribeActive();

        // keyprov SUBACK 失败被降级：不分发；FOTA 不受影响
        manager.handleMessage("vehicle/DEVICE001/up/keyprov", new MqttMessage(new byte[]{1}));
        manager.handleMessage("vehicle/DEVICE001/up/fota", new MqttMessage(new byte[]{1}));

        verify(keyprovIngressAdapter, never()).handleUplink(any(), any());
        verify(uplinkBridge).handleUplink(any(), any());
        assertEquals(0L, metrics.gaugeValue(
                "vagw_mqtt_subscription_state{group=vagw,service=security.keyprov,filter=vehicle/+/up/keyprov}"));
        assertEquals(1L, metrics.get("vagw_mqtt_suback_total{group=vagw,service=security.keyprov,result=failure}"));
        assertEquals(1L, metrics.get("vagw_mqtt_suback_total{group=vagw,service=fota,result=success}"));
        assertEquals(1L, metrics.get("vagw_mqtt_uplink_rejected_total{service=keyprov,reason=filter_not_subscribed}"));
    }

    @Test
    void handleMessage_invalidTopicFormat_shouldRejectWithoutDispatch() {
        manager.handleMessage("invalid/topic", new MqttMessage(new byte[]{1}));

        verify(uplinkBridge, never()).handleUplink(any(), any());
        assertEquals(1L, metrics.get("vagw_mqtt_uplink_rejected_total{service=unknown,reason=invalid_topic_format}"));
    }

    @Test
    void handleMessage_notSubscribedSegment_shouldReject() {
        manager.handleMessage("vehicle/DEVICE001/up/fota", new MqttMessage(new byte[]{1}));

        verify(uplinkBridge, never()).handleUplink(any(), any());
        assertEquals(1L, metrics.get("vagw_mqtt_uplink_rejected_total{service=fota,reason=filter_not_subscribed}"));
    }
}
