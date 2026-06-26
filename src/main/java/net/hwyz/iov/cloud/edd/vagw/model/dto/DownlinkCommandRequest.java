package net.hwyz.iov.cloud.edd.vagw.model.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DownlinkCommandRequest {
    @NotBlank
    private String service;

    @NotBlank
    private byte[] payload;

    @NotBlank
    private String msgId;

    private Long ttl;
}
