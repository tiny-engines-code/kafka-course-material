select count(1) from kafka_notification_status;

-- average time in service
select delivery_channel, step, count(1), round(avg(EXTRACT(MILLISECONDS FROM  publish_time-received_time))) as duration_secs
from kafka_service_status
group by delivery_channel, step;

-- average completion time
select notification_type, delivery_channel, step, status, count(1), round(avg(EXTRACT(SECONDS FROM  end_time-start_time)) ) as duration_secs from kafka_notification_status
where status='success'
group by notification_type, delivery_channel, step, status;

-- successes and failures by comm_id
select notification_type, delivery_channel,  status, count(1), round(avg(EXTRACT(SECONDS FROM  end_time-start_time)) ) as duration_secs
from kafka_notification_status
where notification_type='generic:campaign'
  and comm_id='cp1045'
group by notification_type, delivery_channel, status
order by status;

-- success failure for a specific user
select notification_type, end_time, delivery_channel,  status, (EXTRACT(SECONDS FROM  end_time-start_time)) as duration_secs
from kafka_notification_status
where upmid='b101d989-194f-4976-9bb7-2596c531105c'
order by status;

--- get all data for a give upmid
select  send_id, step, received_time, publish_time
from kafka_service_status
where send_id='ee319106-40d6-4bd6-8a71-ee4aa5eea801'
order by received_time;

--- queue-time and service time queries
with
    service_lags as (
        select   notification_type, step, received_time, publish_time,
                 rank() over (partition by send_id order by send_id, received_time) as rowid,
                 lag(publish_time) over (partition by send_id  order by send_id, publish_time) as previous_publish
        from kafka_service_status),
    service_times as (
        select  notification_type, step, received_time, publish_time,
                EXTRACT(MILLISECONDS FROM  received_time-previous_publish) as ms_waiting_in_que,
                EXTRACT(MILLISECONDS FROM  publish_time-received_time) as ms_in_service
        from service_lags )
select  step, notification_type, avg(ms_in_service) averge_queue_waiting_ms,  avg(ms_waiting_in_que) average_time_in_service_ms
from service_times
where notification_type = 'generic:trigger'
group by notification_type, step
order by step;
