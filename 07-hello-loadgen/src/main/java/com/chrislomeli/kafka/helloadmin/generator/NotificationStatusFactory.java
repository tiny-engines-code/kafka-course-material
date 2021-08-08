package com.chrislomeli.kafka.helloadmin.generator;

import com.chrislomeli.kafka.helloadmin.registry.GenericRecordFactory;
import com.github.javafaker.Faker;
import org.apache.avro.generic.GenericRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class NotificationStatusFactory<V> implements IDatumFactory {
    Map<String, String> attributes;
    Faker faker;

    public NotificationStatusFactory() {
        faker = new Faker();
        setDefaultAttributes();
    }

    public void setDefaultAttributes() {
        attributes = new HashMap<>();
        attributes.put("comm_id", "cp1045");
        attributes.put("delivery_channel", "email");
        attributes.put("locale_country", "US");
        attributes.put("locale_language", "en");
        attributes.put("notification_type", "generic:campaign");
        attributes.put("recipient_email_domain", "gmail.com");
        attributes.put("notification_status", "success");
        attributes.put("vendor", "SES");
    }

    public List<V> next() {
        int terminal = 5;
        Random rand = new Random();
        String user = faker.name().username();
        String upmid = UUID.randomUUID().toString();
        List<GenericRecord> userNotifications = new ArrayList<>();
        for (int i = 0; i < terminal; i++) {
            userNotifications.add(nextStep(user, i));
        }
        return (List<V>) userNotifications;
    }

    public GenericRecord nextStep(String upmid, int level) {
        NotificationStatus ns = new NotificationStatus();
        Random rand = new Random();

        String comm_id = attributes.get("comm_id");
        String delivery_channel = attributes.get("delivery_channel");
        String locale_country = attributes.get("locale_country");
        String locale_language = attributes.get("locale_language");
        String notification_type = attributes.get("notification_type");
        String vendor = attributes.get("vendor");
        String status = attributes.get("notification_status");
        String destination = attributes.get("recipient_email_domain");

        ns.setSend_id(UUID.randomUUID().toString());
        ns.setComm_id(comm_id);
        ns.setCp_code(String.format("cds_v2_perf_%s_%s}", comm_id, upmid));
        ns.setDelivery_channel(delivery_channel);
        ns.setDestination(destination);
        ns.setLocale_country(locale_country);
        ns.setLocale_language(locale_language);
        ns.setNotification_type(notification_type);
        ns.setRequest_id(String.format("%s-%s-%s-%s", notification_type, comm_id, upmid, delivery_channel));
        ;
        ns.setStatus("success");
        ns.setMessage("");
        ns.setUpmid(upmid);
        ns.setUser_notification_id(String.format("%s-%s-%s", notification_type, comm_id, upmid));
        ;
        ns.setVendor(vendor);

        // literals
        ns.setApi_version("2.0.0");
        ns.setAwshost("ip-10-8-236-57-us-east-1a");
        ns.setAppteam("Notifications:CDS");
        ns.setEventtype("country_ONENIKE (ONENIKE)");
        ns.setEnvironment("perf");
        ns.setTag("ONENIKE");
        ns.setVersion("v2");
        ns.setMetric(3);

        // time
        Instant received_time = Instant.now();
        Instant start_time = received_time.minusMillis(level * 250);
        Instant publish_time = received_time.plusMillis(5);
        long time_taken = Duration.between(start_time, publish_time).toMillis();

        ns.setReceived_time(java.time.Instant.ofEpochMilli(received_time.toEpochMilli()));
        ns.setStart_time(java.time.Instant.ofEpochMilli(start_time.toEpochMilli()));
        ns.setPublish_time(java.time.Instant.ofEpochMilli(publish_time.toEpochMilli()));
        ns.setEnd_time(java.time.Instant.ofEpochMilli(publish_time.toEpochMilli()));
        ns.setTime_taken(time_taken);


        // conditionals
        String record_type = "processing";
        if (ns.getStatus().equals("failure") || level > 3) {
            record_type = "terminal";
        }
        ns.setRecord_type(record_type);

        switch (level) {
            case 0:
                ns.setStep("accept_request");
                ns.setApp("validatesvc");
                break;
            case 1:
                ns.setStep("gather_data");
                ns.setApp("gatherdatasvc");
                break;
            case 2:
                ns.setStep("determine_target");
                ns.setApp("determinetargetsvc");
                break;
            case 3:
                ns.setStep("render_notification");
                ns.setApp("crssvc");
                ns.setAppteam("Notifications:CRS");
                break;
            default:
                ns.setStep("transporter");
                ns.setApp("deliversvc");
                break;
        }
        ns.setApplication(ns.getApp());
        ns.setShort_name(String.format("%s:%s", ns.getAppteam(), ns.getApp()));

        return GenericRecordFactory.fromPojo(ns);
    }


}
