package com.chrislomeli.kafka.kafkaspring.generator;

import com.chrislomeli.kafka.kafkaspring.registry.GenericRecordFactory;
import com.github.javafaker.Faker;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class NotificationStatusFactory {

    static final int serviceRelativeStartMs = 1000;
    static final int timeInService = 400; // less than serviceRelativeStartMs

    Map<String, String> attributes;
    Faker faker;

    String serviceTopic;
    String notificationTopic;

    List<Integer> cheesyRandom ;

    public NotificationStatusFactory(String notificationTopic, String serviceTopic) {
        faker = new Faker();
        setDefaultAttributes();
        this.serviceTopic = serviceTopic;
        this.notificationTopic = notificationTopic;
        cheesyRandom = new ArrayList<Integer>() ;
        for (int i=0; i<97; i++) cheesyRandom.add(5);
        for (int i=1; i<5; i++) cheesyRandom.add(i);

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

    public List<GenericRequests> next() {
        Random rand = new Random();
        int lastService = cheesyRandom.get(rand.nextInt(cheesyRandom.size()));
        String sendId = UUID.randomUUID().toString();
        String user = faker.name().username();
        String upmid = UUID.randomUUID().toString();
        Instant start_time = Instant.now();
        List<GenericRequests> userNotifications = new ArrayList<>();
        for (int i = 0; i < lastService; i++) {
            boolean terminal = (i == lastService-1);
            String status = (terminal && lastService < 5) ? "failure": "success";
            userNotifications.add(nextStep(start_time, sendId, upmid, i, status, terminal));
        }
        return userNotifications;
    }

    public GenericRequests nextStep(Instant startTime, String sendId, String upmid, int stepOrdinal, String status, boolean terminal) {

        if (terminal)
            return nextNotificationStatus( startTime, sendId, upmid, stepOrdinal, status);
        else
            return nextServiceStatus( startTime, sendId, upmid, stepOrdinal, status);

    }

    public GenericRequests nextServiceStatus(Instant startTime, String sendId, String upmid, int stepNumber, String status) {
        ServiceStatus ns = new ServiceStatus();
        String notification_type = attributes.get("notification_type");
        String delivery_channel = attributes.get("delivery_channel");
        // time
        Instant received_time = startTime.plusMillis(stepNumber * serviceRelativeStartMs);
        Instant publish_time = received_time.plusMillis(timeInService);
        long time_taken = Duration.between(received_time, publish_time).toMillis();

        ns.setSend_id(sendId);
        ns.setNotification_type(notification_type);
        ns.setStatus(status);
        ns.setReceived_time(received_time);
        ns.setStart_time(startTime);
        ns.setPublish_time(publish_time);
        ns.setEnd_time(publish_time);
        ns.setTime_taken(time_taken);
        ns.setDelivery_channel(delivery_channel);
        ns.setUpmid(upmid);

        // conditionals
        String record_type = "processing";
        if (ns.getStatus().equals("failure") || stepNumber > 3) {
            record_type = "terminal";
        }
        ns.setRecord_type(record_type);

        switch (stepNumber) {
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
                break;
            default:
                ns.setStep("transporter");
                ns.setApp("deliversvc");
                break;
        }
        return new GenericRequests( new GenericRequest(serviceTopic, GenericRecordFactory.fromPojo(ns)));
    }

    public GenericRequests nextNotificationStatus(Instant startTime, String sendId, String upmid, int stepNumber, String status) {
        NotificationStatus notificationStatus = new NotificationStatus();
        // Notification Status Record
        String comm_id = attributes.get("comm_id");
        String delivery_channel = attributes.get("delivery_channel");
        String locale_country = attributes.get("locale_country");
        String locale_language = attributes.get("locale_language");
        String notification_type = attributes.get("notification_type");
        String vendor = attributes.get("vendor");
        String destination = attributes.get("recipient_email_domain");
        // time
        Instant received_time = startTime.plusMillis(stepNumber * serviceRelativeStartMs);
        Instant publish_time = received_time.plusMillis(timeInService);
        long time_taken = Duration.between(received_time, publish_time).toMillis();


        notificationStatus.setSend_id(sendId);
        notificationStatus.setComm_id(comm_id);
        notificationStatus.setCp_code(String.format("cds_v2_perf_%s_%s}", comm_id, upmid));
        notificationStatus.setDelivery_channel(delivery_channel);
        notificationStatus.setDestination(destination);
        notificationStatus.setLocale_country(locale_country);
        notificationStatus.setLocale_language(locale_language);
        notificationStatus.setNotification_type(notification_type);
        notificationStatus.setRequest_id(String.format("%s-%s-%s-%s", notification_type, comm_id, upmid, delivery_channel));
        notificationStatus.setStatus(status);
        notificationStatus.setMessage("");
        notificationStatus.setUpmid(upmid);
        notificationStatus.setUser_notification_id(String.format("%s-%s-%s", notification_type, comm_id, upmid));
        notificationStatus.setVendor(vendor);
        notificationStatus.setApi_version("2.0.0");
        notificationStatus.setAwshost("ip-10-8-236-57-us-east-1a");
        notificationStatus.setAppteam("Notifications:CDS");
        notificationStatus.setEventtype("country_ONENIKE (ONENIKE)");
        notificationStatus.setEnvironment("perf");
        notificationStatus.setTag("ONENIKE");
        notificationStatus.setVersion("v2");
        notificationStatus.setMetric(3);
        notificationStatus.setReceived_time(received_time);
        notificationStatus.setStart_time(startTime);
        notificationStatus.setPublish_time(publish_time);
        notificationStatus.setEnd_time(publish_time);
        notificationStatus.setTime_taken(time_taken);

        // conditionals
        String record_type = "processing";
        if (notificationStatus.getStatus().equals("failure") || stepNumber > 3) {
            record_type = "terminal";
        }
        notificationStatus.setRecord_type(record_type);

        switch (stepNumber) {
            case 0:
                notificationStatus.setStep("accept_request");
                notificationStatus.setApp("validatesvc");
                break;
            case 1:
                notificationStatus.setStep("gather_data");
                notificationStatus.setApp("gatherdatasvc");
                break;
            case 2:
                notificationStatus.setStep("determine_target");
                notificationStatus.setApp("determinetargetsvc");
                break;
            case 3:
                notificationStatus.setStep("render_notification");
                notificationStatus.setApp("crssvc");
                notificationStatus.setAppteam("Notifications:CRS");
                break;
            default:
                notificationStatus.setStep("transporter");
                notificationStatus.setApp("deliversvc");
                break;
        }
        notificationStatus.setApplication(notificationStatus.getApp());
        notificationStatus.setShort_name(String.format("%s:%s", notificationStatus.getAppteam(), notificationStatus.getApp()));

        // also return a Service Status Record because we are also at a service that has not been reported
        ServiceStatus serviceStatus = new ServiceStatus();
        serviceStatus.setSend_id(notificationStatus.getSend_id());
        serviceStatus.setNotification_type(notificationStatus.getNotification_type());
        serviceStatus.setRecord_type(notificationStatus.getRecord_type());
        serviceStatus.setDelivery_channel(notificationStatus.getDelivery_channel());
        serviceStatus.setStatus(notificationStatus.getStatus());
        serviceStatus.setUpmid(notificationStatus.getUpmid());
        serviceStatus.setReceived_time(notificationStatus.getReceived_time());
        serviceStatus.setStart_time(notificationStatus.getStart_time());
        serviceStatus.setPublish_time(notificationStatus.getPublish_time());
        serviceStatus.setEnd_time(notificationStatus.getEnd_time());
        serviceStatus.setTime_taken(notificationStatus.getTime_taken());
        serviceStatus.setStep(notificationStatus.getStep());
        serviceStatus.setApp(notificationStatus.getApp());
        return new GenericRequests(
                new GenericRequest(serviceTopic, GenericRecordFactory.fromPojo(serviceStatus)) ,
                new GenericRequest(notificationTopic, GenericRecordFactory.fromPojo(notificationStatus)) );
    }

}
