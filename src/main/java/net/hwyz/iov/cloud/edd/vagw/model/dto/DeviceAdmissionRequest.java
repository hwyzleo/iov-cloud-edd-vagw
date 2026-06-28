package net.hwyz.iov.cloud.edd.vagw.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeviceAdmissionRequest {

    private String uid;
    private String certSerial;
    private String clientIp;
    private String protocolVersion;
}
