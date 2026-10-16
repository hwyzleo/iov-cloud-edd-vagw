package net.hwyz.iov.cloud.edd.vagw.infrastructure.keyprov;

import lombok.RequiredArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.config.KeyprovProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * keyprov 传输层短期去重（EDD-VAGW-DSN-CR-011 §3.4/§7）。
 * <p>
 * 只对重复 {@code message_id} 做有界短期去重，不创建新业务身份、不改写 requestId；
 * 相同 messageId + 相同摘要视为 QoS1 重复（跳过重复 VMD 调用），不同摘要按冲突 fail-closed。
 * 不落盘、不进 Kafka/Redis/DB/DLQ。
 * </p>
 */
@Component
@ConditionalOnProperty(prefix = "vagw.keyprov", name = "enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class KeyprovInbox {

    private final KeyprovProperties properties;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    public enum State { NEW, DUPLICATE, CONFLICT }

    public State record(String messageId, String digest) {
        if (messageId == null || messageId.isBlank()) {
            return State.NEW;
        }
        long now = System.currentTimeMillis();
        evict(now);
        Entry existing = entries.get(messageId);
        if (existing != null) {
            if (existing.digest().equals(digest)) {
                return State.DUPLICATE;
            }
            return State.CONFLICT;
        }
        entries.put(messageId, new Entry(digest, now));
        return State.NEW;
    }

    public void forget(String messageId) {
        if (messageId != null) {
            entries.remove(messageId);
        }
    }

    private void evict(long now) {
        long ttlMillis = properties.getInboxTtlSeconds() * 1000L;
        entries.entrySet().removeIf(e -> now - e.getValue().at() > ttlMillis);
        int max = properties.getInboxMaxEntries();
        if (entries.size() > max) {
            entries.entrySet().stream()
                    .sorted(Comparator.comparingLong(e -> e.getValue().at()))
                    .limit(entries.size() - max)
                    .map(Map.Entry::getKey)
                    .toList()
                    .forEach(entries::remove);
        }
    }

    record Entry(String digest, long at) {
    }
}
