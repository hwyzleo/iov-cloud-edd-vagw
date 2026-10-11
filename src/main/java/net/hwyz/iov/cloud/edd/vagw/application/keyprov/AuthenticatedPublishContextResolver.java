package net.hwyz.iov.cloud.edd.vagw.application.keyprov;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.hwyz.iov.cloud.edd.vagw.model.entity.SessionInfo;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.BrokerAuthenticatedPublishContext;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovContextSource;
import net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason;
import net.hwyz.iov.cloud.edd.vagw.service.SessionService;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Locale;
import java.util.Optional;

import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_SESSION_CONTEXT_MISSING;
import static net.hwyz.iov.cloud.edd.vagw.model.keyprov.KeyprovReason.KEYPROV_SESSION_STALE;

/**
 * 已认证发布上下文解析器（EDD-VAGW-DSN-CR-011 §3.2，{@code ACL_SESSION_CORRELATED}）。
 * <p>
 * 解析优先级落地为：EMQX ACL 以已验证客户端证书 CN 强制 {@code Topic device key == 认证身份}，
 * VAGW 再以准入/连接事件建立的会话不可变快照（含认证期 certSerial 与 connection generation）
 * 关联 Topic hsmUid。Topic hsmUid 只用于交叉校验，绝不单独作为认证来源，更不信任 Envelope
 * device_id 自报身份。
 * </p>
 * <p>
 * 当前 Paho v3.1.1 客户端共享订阅无法携带 EMQX publisher metadata；若后续 EMQX Rule/Hook
 * 可注入 publisher/session 元数据，可新增来源而不改变下游契约。
 * </p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AuthenticatedPublishContextResolver {

    private final SessionService sessionService;

    public Resolution resolve(String topicHsmUid, Instant receivedAt) {
        if (topicHsmUid == null || topicHsmUid.isBlank()) {
            return Resolution.fail(KEYPROV_SESSION_CONTEXT_MISSING);
        }
        String hsmUid = topicHsmUid.trim().toUpperCase(Locale.ROOT);
        Optional<SessionInfo> sessionOpt = sessionService.getSessionByDeviceSn(hsmUid);
        if (sessionOpt.isEmpty()) {
            return Resolution.fail(KEYPROV_SESSION_CONTEXT_MISSING);
        }
        SessionInfo session = sessionOpt.get();
        if (!session.isOnline()) {
            return Resolution.fail(KEYPROV_SESSION_STALE);
        }
        BrokerAuthenticatedPublishContext context = new BrokerAuthenticatedPublishContext(
                KeyprovContextSource.ACL_SESSION_CORRELATED,
                hsmUid,
                session.getClientId(),
                session.getCertSerial(),
                session.getCertFingerprint(),
                session.getConnectionGeneration(),
                session.getClientId(),
                receivedAt);
        return Resolution.ok(context);
    }

    public record Resolution(BrokerAuthenticatedPublishContext context, KeyprovReason failureReason) {
        public static Resolution ok(BrokerAuthenticatedPublishContext context) {
            return new Resolution(context, null);
        }

        public static Resolution fail(KeyprovReason reason) {
            return new Resolution(null, reason);
        }
    }
}
