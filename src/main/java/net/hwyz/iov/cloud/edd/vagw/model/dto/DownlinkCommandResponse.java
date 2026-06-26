package net.hwyz.iov.cloud.edd.vagw.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DownlinkCommandResponse {
    private boolean accepted;
    private String msgId;
    private String reason;
    private Integer errorCode;
}
