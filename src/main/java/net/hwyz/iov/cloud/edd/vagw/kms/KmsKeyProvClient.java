package net.hwyz.iov.cloud.edd.vagw.kms;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "kms-keyprov", url = "${kms.keyprov.url}", fallbackFactory = KmsKeyProvClientFallbackFactory.class)
public interface KmsKeyProvClient {

    @PostMapping("/api/service/keyprov/v1/issue")
    KeyProvIssueResult issue(@RequestBody KeyProvIssueRequest request);

    record KeyProvIssueRequest(
            String device_sn,
            String certSerial,
            String bizDomain,
            String usage,
            byte[] requestPayload
    ) {}

    record KeyProvIssueResult(
            byte[] wrappedKey,
            String keyId,
            int keyVersion,
            String alg,
            long validUntil,
            byte[] kdfParams
    ) {}
}
