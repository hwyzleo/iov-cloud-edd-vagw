package net.hwyz.iov.cloud.edd.vagw.kms;

public class KmsKeyProvException extends RuntimeException {

    public KmsKeyProvException(String message) {
        super(message);
    }

    public KmsKeyProvException(String message, Throwable cause) {
        super(message, cause);
    }
}
