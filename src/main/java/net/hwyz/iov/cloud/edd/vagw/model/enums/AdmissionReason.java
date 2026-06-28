package net.hwyz.iov.cloud.edd.vagw.model.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum AdmissionReason {

    UID_UNKNOWN("UID_UNKNOWN"),
    UNBOUND("UNBOUND"),
    CERT_REVOKED("CERT_REVOKED"),
    CERT_EXPIRED("CERT_EXPIRED"),
    DEVICE_RETIRED("DEVICE_RETIRED"),
    DEVICE_BLOCKED("DEVICE_BLOCKED"),
    DEPENDENCY_UNAVAILABLE("DEPENDENCY_UNAVAILABLE");

    private final String value;

    AdmissionReason(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
