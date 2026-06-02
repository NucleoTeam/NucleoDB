package com.nucleodb.library.mqs.kafka;

import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaProducerHandler extends ProducerHandler{
    private static Logger logger = Logger.getLogger(KafkaProducerHandler.class.getName());

    private KafkaProducer producer;


    public KafkaProducerHandler(MQSSettings settings, String servers) {
        super(settings);
        createTopics();
        producer = createProducer(servers);
    }

    private KafkaProducer createProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 25);
        // Idempotent producer prevents duplicate records when retries fire (acks=all + retries>0
        // without idempotence can silently duplicate events). Requires max.in.flight <= 5 (default).
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
//        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 1500);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 200);
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 200);
        return new KafkaProducer(props);
    }
    public KafkaProducer getProducer() {
        return producer;
    }

    public void createTopics() {
        Properties props = new Properties();
        KafkaSettings settings = (KafkaSettings) getSettings();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getServers());
        AdminClient client = KafkaAdminClient.create(props);

        String topic = super.getTopic();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ListTopicsResult listTopicsResult = client.listTopics();
            listTopicsResult.names().whenComplete((names, f)->{
                if(f!=null){
                    f.printStackTrace();
                }
                if (names.stream().filter(name -> name.equals(topic)).count() == 0) {
                    logger.info(String.format("kafka topic not found for %s", topic));

                    final NewTopic newTopic = new NewTopic(topic, settings.partitions, (short) settings.replicas);
                    newTopic.configs(new TreeMap<>(){{
                        put(TopicConfig.RETENTION_MS_CONFIG, "-1");
                        put(TopicConfig.RETENTION_MS_CONFIG, "-1");
                        put(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
                    }});
                    CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
                    createTopicsResult.all().whenComplete((c, e) -> {
                        if (e != null) {
                            e.printStackTrace();
                            logger.severe(e.getMessage());
                        }

                        countDownLatch.countDown();
                    });
                }else{
                    countDownLatch.countDown();
                }
            });
        } catch (Exception e) {
            // Fail the producer construction loudly instead of killing the host JVM with System.exit.
            logger.log(Level.SEVERE, "failed to verify/create Kafka topic " + topic, e);
            throw new RuntimeException("failed to verify/create Kafka topic " + topic, e);
        }

        try {
            countDownLatch.await(60, TimeUnit.SECONDS);
            CountDownLatch countDownLatchCreatedCheck = new CountDownLatch(1);
            ListTopicsResult listTopicsResult = client.listTopics();
            listTopicsResult.names().whenComplete((names, f)->{
                if(f!=null){
                    f.printStackTrace();
                    logger.severe(f.getMessage());
                }
                if (names.stream().filter(name -> name.equals(topic)).count() == 0) {
                    logger.severe("topic not created "+topic);
                }
                countDownLatchCreatedCheck.countDown();
            });
            countDownLatchCreatedCheck.await();
        } catch (Exception e) {
            // Fail the producer construction loudly instead of killing the host JVM with System.exit.
            logger.log(Level.SEVERE, "failed to verify/create Kafka topic " + topic, e);
            throw new RuntimeException("failed to verify/create Kafka topic " + topic, e);
        }
        client.close();
    }

    @Override
    public void push(String key, long version, Modify modify, Callback callback){
        // Send directly on the async, thread-safe KafkaProducer. Spawning a thread per message
        // (the previous behaviour) let version N+1 reach the broker before version N, breaking
        // Kafka's per-partition ordering guarantee and manufacturing out-of-order events.
        try {
            ProducerRecord record = new ProducerRecord(
                super.getTopic(),
                key,
                modify.getClass().getSimpleName() + Serializer.getObjectMapper().getOm().writeValueAsString(modify)
            );
            record.headers().add("version", Long.valueOf(version).toString().getBytes());

            getProducer().send(record, (e, ex) -> {
                if (ex != null) {
                    // Do not System.exit: this is an embedded library and a transient broker
                    // error must not kill the host JVM. Surface the failure to the caller.
                    logger.log(Level.SEVERE, "failed to publish modification for key " + key, ex);
                }
                if (callback != null) callback.onCompletion(e, ex);
            });
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to enqueue modification for key " + key, e);
            if (callback != null) callback.onCompletion(null, e);
        }
    }
    @Override
    public void push(String key, String message){
        try {
            ProducerRecord record = new ProducerRecord(
                super.getTopic(),
                key,
                message
            );
            getProducer().send(record, (e, ex) -> {
                if (ex != null) {
                    logger.log(Level.SEVERE, "failed to publish message for key " + key, ex);
                }
            });
        } catch (Exception e) {
            logger.log(Level.SEVERE, "failed to enqueue message for key " + key, e);
        }
    }
}