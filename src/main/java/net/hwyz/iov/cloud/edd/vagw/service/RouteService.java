package net.hwyz.iov.cloud.edd.vagw.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class RouteService {

    // service name -> Kafka topic mapping
    private final Map<String, String> routeTable = new ConcurrentHashMap<>(
            Map.of(
                    "remotecontrol", "iov.vagw.up.remotecontrol",
                    "remotecontrol_ack", "iov.vagw.up.remotecontrol.ack",
                    "keyprov", "iov.vagw.up.keyprov"
            )
    );

    public String getTopic(String service) {
        return routeTable.get(service);
    }

    public void addRoute(String service, String kafkaTopic) {
        routeTable.put(service, kafkaTopic);
        log.info("Route added: service={} -> topic={}", service, kafkaTopic);
    }

    public boolean hasRoute(String service) {
        return routeTable.containsKey(service);
    }
}
