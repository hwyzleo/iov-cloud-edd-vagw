package net.hwyz.iov.cloud.edd.vagw.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionDecision;
import net.hwyz.iov.cloud.edd.vagw.model.enums.AdmissionReason;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeviceAdmissionResult {

    private AdmissionDecision decision;
    private AdmissionReason reason;
    private String vin;
    private String bindStatus;
    private String certStatus;
    private String deviceStatus;
    private Long bindVersion;
    private Instant updatedAt;
}
