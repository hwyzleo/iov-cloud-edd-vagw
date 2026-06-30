package net.hwyz.iov.cloud.edd.vagw.kms;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "kms-keyprov", url = "${kms.keyprov.url}", fallbackFactory = KmsKeyProvClientFallbackFactory.class)
public interface KmsKeyProvClient {

    @PostMapping("/api/service/keyprov/v1/issue")
    KeyProvIssueResult issue(@RequestBody KeyProvIssueRequest request);

    record KeyProvIssueRequest(
            @JsonProperty("device_sn") String deviceSn,
            @JsonProperty("cert_serial") String certSerial,
            @JsonProperty("biz_domain") String bizDomain,
            @JsonProperty("usage") String usage,
            @JsonProperty("request_payload") byte[] requestPayload
    ) {}

    record KeyProvIssueResult(
            @JsonProperty("wrapped_key") byte[] wrappedKey,
            @JsonProperty("key_id") String keyId,
            @JsonProperty("key_version") int keyVersion,
            @JsonProperty("alg") String alg,
            @JsonProperty("valid_until") long validUntil,
            @JsonProperty("kdf_params") byte[] kdfParams
    ) {}
}
