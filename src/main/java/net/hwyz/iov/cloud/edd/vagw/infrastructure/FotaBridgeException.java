package net.hwyz.iov.cloud.edd.vagw.infrastructure;

/**
 * FOTA 桥接基础设施异常（Kafka produce / DLQ / Inbox 等）。
 */
public class FotaBridgeException extends RuntimeException {

    public FotaBridgeException(String message) {
        super(message);
    }

    public FotaBridgeException(String message, Throwable cause) {
        super(message, cause);
    }
}
