package net.hwyz.iov.cloud.edd.vagw.model.keyprov;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningAck;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningAckResult;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningRequest;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningResponse;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningStatusRequest;
import net.hwyz.iov.security.keyprov.v1.KeyProvisioningStatusResponse;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * keyprov canonical 六类消息精确 registry（EDD-VAGW-DSN-CR-011 §3.3/§3.6）。
 * <p>
 * payload_type 为 PAR-PROTO proto-keyprov 完整类型名（{@code security.keyprov.v1.<MessageName>}）；
 * 只按整串精确匹配，禁止 JSON、短名、通配符、动态 reflection fallback 或未知关键枚举。
 * </p>
 */
public enum KeyprovMessageType {

    KEY_PROVISIONING_REQUEST("security.keyprov.v1.KeyProvisioningRequest", Direction.UP,
            KeyProvisioningRequest.parser()),
    KEY_PROVISIONING_ACK("security.keyprov.v1.KeyProvisioningAck", Direction.UP,
            KeyProvisioningAck.parser()),
    KEY_PROVISIONING_STATUS_REQUEST("security.keyprov.v1.KeyProvisioningStatusRequest", Direction.UP,
            KeyProvisioningStatusRequest.parser()),
    KEY_PROVISIONING_RESPONSE("security.keyprov.v1.KeyProvisioningResponse", Direction.DOWN,
            KeyProvisioningResponse.parser()),
    KEY_PROVISIONING_ACK_RESULT("security.keyprov.v1.KeyProvisioningAckResult", Direction.DOWN,
            KeyProvisioningAckResult.parser()),
    KEY_PROVISIONING_STATUS_RESPONSE("security.keyprov.v1.KeyProvisioningStatusResponse", Direction.DOWN,
            KeyProvisioningStatusResponse.parser());

    /** 消息方向（上行 = 车端 → VAGW → VMD；下行 = VMD → VAGW → 车端）。 */
    public enum Direction { UP, DOWN }

    private static final Map<String, KeyprovMessageType> BY_FULL_NAME =
            Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(KeyprovMessageType::fullName, Function.identity()));

    private final String fullName;
    private final Direction direction;
    private final Parser<? extends MessageLite> parser;

    KeyprovMessageType(String fullName, Direction direction, Parser<? extends MessageLite> parser) {
        this.fullName = fullName;
        this.direction = direction;
        this.parser = parser;
    }

    public String fullName() {
        return fullName;
    }

    public Direction direction() {
        return direction;
    }

    public Parser<? extends MessageLite> parser() {
        return parser;
    }

    public boolean isUpstream() {
        return direction == Direction.UP;
    }

    /**
     * 上行 PayloadType 期望的 canonical 下行 PayloadType（EDD-VAGW-DSN-CR-011 §6 处理矩阵）。
     */
    public KeyprovMessageType expectedDownlink() {
        return switch (this) {
            case KEY_PROVISIONING_REQUEST -> KEY_PROVISIONING_RESPONSE;
            case KEY_PROVISIONING_ACK -> KEY_PROVISIONING_ACK_RESULT;
            case KEY_PROVISIONING_STATUS_REQUEST -> KEY_PROVISIONING_STATUS_RESPONSE;
            default -> throw new IllegalStateException("not an upstream keyprov type: " + fullName);
        };
    }

    public static Optional<KeyprovMessageType> fromFullName(String fullName) {
        if (fullName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_FULL_NAME.get(fullName));
    }
}
