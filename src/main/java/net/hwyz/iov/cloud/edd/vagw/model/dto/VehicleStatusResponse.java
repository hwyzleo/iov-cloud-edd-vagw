package net.hwyz.iov.cloud.edd.vagw.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VehicleStatusResponse {
    private boolean online;
    private String lastOnlineAt;
    private String lastOfflineAt;
}
