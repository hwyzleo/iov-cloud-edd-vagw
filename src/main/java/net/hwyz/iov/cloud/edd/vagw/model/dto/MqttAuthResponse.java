package net.hwyz.iov.cloud.edd.vagw.model.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MqttAuthResponse {
    @JsonProperty("result")
    private String result;  // "allow" or "deny"

    @JsonProperty("is_superuser")
    @Builder.Default
    private Boolean isSuperuser = false;

    @JsonProperty("acl")
    private List<AclRule> acl;

    @JsonProperty("reason")
    private String reason;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AclRule {
        @JsonProperty("permission")
        private String permission;  // "allow"

        @JsonProperty("action")
        private String action;  // "publish" or "subscribe"

        @JsonProperty("topic")
        private String topic;  // "vehicle/{VIN}/#"
    }
}
