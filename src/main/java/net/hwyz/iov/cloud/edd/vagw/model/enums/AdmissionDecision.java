package net.hwyz.iov.cloud.edd.vagw.model.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum AdmissionDecision {

    ALLOW("ALLOW"),
    DENY("DENY");

    private final String value;

    AdmissionDecision(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
