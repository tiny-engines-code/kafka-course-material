package com.chrislomeli.springsandbox.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationStatus {
    private static final long serialVersionUID = 9188160226931610406L;

    /**
     * NGAP required unique id for this request, this user, and this channel (concat request_id + user_notification_id)
     */
    public String send_id;

    /**
     * UTC time in milliseconds when this service began processing this record
     */
    public java.time.Instant start_time;

    /**
     * The time in UTC millseconds when this service existed
     */
    public java.time.Instant end_time;

    /**
     * UTC time in milliseconds that this service received this request
     */
    public java.time.Instant received_time;

    /**
     * UTC time in milliseconds that this service published its output
     */
    public java.time.Instant publish_time;

    /**
     * CDS API version number
     */
    public String api_version;

    /**
     * The service creating this record
     */
    public String app;

    /**
     * The service creating this record
     */
    public String application;

    /**
     * The NCP team - always Notifications:CDS
     */
    public String appteam;

    /**
     * The host where the service was running
     */
    public String awshost;

    /**
     * NCP com_id
     */
    public String comm_id;

    /**
     * NCP cp_code
     */
    public String cp_code;

    /**
     * EMAIL,PUSH,SMS,INBOX
     */
    public String delivery_channel;

    /**
     * NGAP required destination is null before transporter and the phone or email or wechatid - depending on the channel
     */
    public String destination;

    /**
     * The environment .e.g perf, prod, etc
     */
    public String environment;

    /**
     * Descriptive event class
     */
    public String eventtype;

    /**
     * Locale e.g. US, UK...
     */
    public String locale_country;

    /**
     * Language e.g. en, fr, ja...
     */
    public String locale_language;

    /**
     * TBD
     */
    public int metric;

    /**
     * NCP notification type
     */
    public String notification_type;

    /**
     * The lifecycle state this record is in
     */
    public String record_type;

    /**
     * NCP request Id
     */
    public String request_id;

    /**
     * App team and service name string
     */
    public String short_name;

    /**
     * The status of this record
     */
    public String status;

    /**
     * NGAP required failure reason, but should just call it message in case it gets used for non failures in future
     */
    public String message;

    /**
     * accept_request, gather_data, determine_target, render, transport
     */
    public String step;

    /**
     * The time in milliseconds that this record took to process within this application
     */
    public long time_taken;


    public String tag;

    /**
     * NCP user identifier
     */
    public String upmid;

    /**
     * NCP user notification identity
     */
    public String user_notification_id;

    /**
     * NGAP required vendor that this notification is directed to - this is null before determine_target
     */
    public String vendor;

    /**
     * CDS version number .e.g. v2
     */
    public String version;

    /*
      Tracing

     */
    public int stepOrdinal;

    @Override
    public String toString() {
        return "NotificationStatus{" +
                "send_id='" + send_id + '\'' +
                ", destination='" + destination + '\'' +
                '}';
    }
}
