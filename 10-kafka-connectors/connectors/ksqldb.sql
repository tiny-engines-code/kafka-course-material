CREATE STREAM cds_send_stream (
    send_id VARCHAR,
    assembly_id VARCHAR,
    render_id VARCHAR,
    upmid VARCHAR,
    status VARCHAR,
    end_time BIGINT,
    message VARCHAR,
    locale_language VARCHAR,
    retries INT,
    locale_country VARCHAR,
    device_type VARCHAR,
    record_type VARCHAR,
    destination VARCHAR,
    senders_email_address VARCHAR,
    vendor VARCHAR
  ) WITH (
    KAFKA_TOPIC='notification_status',
    VALUE_FORMAT='AVRO'
  );

CREATE STREAM CDS_NGAP_SENDS AS
      SELECT
        send_id ,
        assembly_id ,
        render_id ,
        upmid ,
        status ,
        end_time as date_sent,
        message AS failure_reason,
        locale_language ,
        retries ,
        locale_country ,
        device_type ,
        destination ,
        senders_email_address ,
        vendor
      FROM  CDS_SEND_STREAM
      WHERE record_type = 'terminal'
      EMIT CHANGES;

