package com.chrislomeli.springsandbox.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ServiceStatus {
    private static final long serialVersionUID = 9188160226931610406L;
    public String send_id;
    public String upmid;
    public String app;
    public String delivery_channel;
    public String notification_type;
    public String record_type;
    public java.time.Instant start_time;
    public java.time.Instant end_time;
    public java.time.Instant received_time;
    public java.time.Instant publish_time;
    public String status;
    public String step;
    public long time_taken;
    public int stepOrdinal;
}
