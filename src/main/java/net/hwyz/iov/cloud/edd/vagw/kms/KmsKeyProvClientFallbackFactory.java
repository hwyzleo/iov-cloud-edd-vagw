package net.hwyz.iov.cloud.edd.vagw.kms;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KmsKeyProvClientFallbackFactory implements FallbackFactory<KmsKeyProvClient> {

    @Override
    public KmsKeyProvClient create(Throwable cause) {
        log.error("KMS keyprov service unavailable", cause);
        return new KmsKeyProvClient() {
            @Override
            public KeyProvIssueResult issue(KeyProvIssueRequest request) {
                log.error("KMS issue fallback triggered for device_sn={}", request.device_sn());
                throw new KmsKeyProvException("KMS service unavailable", cause);
            }
        };
    }
}
