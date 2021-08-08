package com.chrislomeli.springsandbox.generator;

import com.chrislomeli.springsandbox.generic.GenericRecordProvider;
import com.chrislomeli.springsandbox.model.NotificationStatus;
import com.chrislomeli.springsandbox.model.ServiceStatus;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class NotificationStatusFactory {

    static final int serviceRelativeStartMs = 3000;
    static final int timeInService = 1000; // less than serviceRelativeStartMs

    Map<String, String> attributes;

    String serviceTopic;
    String notificationTopic;

    List<Integer> sillyWeightedRandom;

    /*
            switch (stepNumber) {
            case 0:
                serviceStatus.setStep("accept_request");
                serviceStatus.setApp("validatesvc");
                break;
            case 1:
                serviceStatus.setStep("gather_data");
                serviceStatus.setApp("gatherdatasvc");
                break;
            case 2:
                serviceStatus.setStep("determine_target");
                serviceStatus.setApp("determinetargetsvc");
                break;
            case 3:
                serviceStatus.setStep("render_notification");
                serviceStatus.setApp("crssvc");
                break;
            default:
                serviceStatus.setStep("transporter");
                serviceStatus.setApp("deliversvc");
                break;
        }


     */
    static Map<Integer, Pair<String,String>> levels;
    static {
        int i=0;
        levels = new HashMap<>();
        levels.put(i++,new ImmutablePair<>("accept_request","validatesvc"));
        levels.put(i++,new ImmutablePair<>("gather_data","gatherdatasvc"));
        levels.put(i++,new ImmutablePair<>("determine_target","determinetargetsvc"));
        levels.put(i++,new ImmutablePair<>("render_notification","crssvc"));
        levels.put(i,new ImmutablePair<>("transporter","deliversvc"));
    }

    public NotificationStatusFactory(String notificationTopic, String serviceTopic) {
        setDefaultAttributes();
        this.serviceTopic = serviceTopic;
        this.notificationTopic = notificationTopic;
        sillyWeightedRandom = new ArrayList<Integer>() ;
        for (int i=0; i<97; i++) sillyWeightedRandom.add(5);
        for (int i=1; i<5; i++) sillyWeightedRandom.add(i);
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

    public List<GenericRequest> next() {
        Random rand = new Random();
        int lastService = sillyWeightedRandom.get(rand.nextInt(sillyWeightedRandom.size()));
        List<GenericRequest> userNotifications = new ArrayList<>();

        NotificationStatus notificationStatus = nextNotificationStatus(lastService-1);
        userNotifications.add(new GenericRequest(notificationTopic, GenericRecordProvider.fromPojo(notificationStatus)));

        // create all of the service statuses all at once for the whole notification - use an integer to sort (e.g. accept-request==1, gather-data=2...)
        for (int i = 0; i < lastService; i++) {
            boolean terminal = (i == lastService-1);
            ServiceStatus svcGr = serviceStatusForStep(notificationStatus, i, terminal);
            userNotifications.add(new GenericRequest(serviceTopic, GenericRecordProvider.fromPojo(svcGr)));
        }
        return userNotifications;
    }

    // todo make a service from a notification
    public ServiceStatus serviceStatusForStep(NotificationStatus notificationStatus, int stepNumber, boolean terminus) {

        // calculate times so that each step has a logical timestamp related to the others (e.g. accept-request is before transporter)
        Instant start_time = notificationStatus.getStart_time();
        Instant received_time = (stepNumber==0) ? start_time : start_time.plusMillis(stepNumber * serviceRelativeStartMs);
        Instant publish_time = received_time.plusMillis(timeInService);
        long time_taken = Duration.between(received_time, publish_time).toMillis();
        String status = (terminus) ? notificationStatus.getStatus() : "success";

        ServiceStatus serviceStatus = new ServiceStatus();
        serviceStatus.setStepOrdinal(stepNumber);
        // copy from notification status
        serviceStatus.setSend_id(notificationStatus.getSend_id());
        serviceStatus.setNotification_type(notificationStatus.getNotification_type());
        serviceStatus.setDelivery_channel(notificationStatus.getDelivery_channel());
        serviceStatus.setUpmid(notificationStatus.getUpmid());
        serviceStatus.setApp(notificationStatus.getApp());

        // specific to this service
        serviceStatus.setStart_time(start_time);
        serviceStatus.setPublish_time(publish_time);
        serviceStatus.setEnd_time(publish_time);
        serviceStatus.setTime_taken(time_taken);
        serviceStatus.setStatus(status);
        serviceStatus.setReceived_time(received_time);
        serviceStatus.setStep(levels.get(stepNumber).getLeft());
        serviceStatus.setApp(levels.get(stepNumber).getRight());

        String record_type = "processing";
        if (serviceStatus.getStatus().equals("failure") || stepNumber > 3) {
            record_type = "terminal";
        }
        serviceStatus.setRecord_type(record_type);
        return serviceStatus;
    }

    public NotificationStatus nextNotificationStatus(int stepNumber) {

        Random rand = new Random();
        String status = (stepNumber < 4) ? "failure": "success";
        String sendId = UUID.randomUUID().toString();
        String upmid = UUID.randomUUID().toString();
//        String destination = faker.internet().emailAddress();
        String destination = String.format("%s@gmail.com", upmid);

        Instant startTime = Instant.now();
        Instant received_time = (stepNumber==0) ? startTime : startTime.plusMillis(stepNumber * serviceRelativeStartMs);
        Instant publish_time = received_time.plusMillis(timeInService);
        long time_taken = Duration.between(received_time, publish_time).toMillis();

        List<GenericRequest> userNotifications = new ArrayList<>();

        NotificationStatus notificationStatus = new NotificationStatus();
        notificationStatus.setStepOrdinal(stepNumber);
        // Notification Status Record
        String comm_id = attributes.get("comm_id");
        String delivery_channel = attributes.get("delivery_channel");
        String locale_country = attributes.get("locale_country");
        String locale_language = attributes.get("locale_language");
        String notification_type = attributes.get("notification_type");
        String vendor = attributes.get("vendor");

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
        notificationStatus.setStep(levels.get(stepNumber).getLeft());
        notificationStatus.setApp(levels.get(stepNumber).getRight());
        notificationStatus.setAppteam((stepNumber==3) ? "Notifications:CRS" : "Notifications:CDS");
        notificationStatus.setApplication(notificationStatus.getApp());
        notificationStatus.setShort_name(String.format("%s:%s", notificationStatus.getAppteam(), notificationStatus.getApp()));

        // conditionals
        String record_type = "processing";
        if (notificationStatus.getStatus().equals("failure") || stepNumber > 3) {
            record_type = "terminal";
        }
        notificationStatus.setRecord_type(record_type);
        return notificationStatus;
    }

}
