package com.nucleodb.library.mqs.kafka.ratis;

import com.nucleodb.library.database.modifications.Modify;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.kafka.KafkaSettings;
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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeerId;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Producer handler that submits operations through Ratis consensus before
 * publishing to Kafka. This ensures all nodes in the cluster agree on the
 * ordering of operations.
 *
 * Write flow:
 * 1. Operation is submitted to Ratis leader via RaftClient
 * 2. Ratis replicates the operation across the cluster via Raft consensus
 * 3. Once committed, the operation is published to Kafka for durable storage
 * 4. All nodes consume from Kafka to apply the committed operations
 */
public class RatisKafkaProducerHandler extends ProducerHandler {
    private static final Logger logger = Logger.getLogger(RatisKafkaProducerHandler.class.getName());

    private final KafkaProducer<String, String> producer;
    private RaftClient raftClient;
    private final RatisKafkaConsumerHandler consumerHandler;

    public RatisKafkaProducerHandler(MQSSettings settings, String servers, RatisKafkaConsumerHandler consumerHandler) {
        super(settings);
        this.consumerHandler = consumerHandler;

        createTopics();
        this.producer = createProducer(servers);
        this.raftClient = createRaftClient();

        logger.info("RatisKafka producer initialized: " + servers);
    }

    private KafkaProducer<String, String> createProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 25);
        return new KafkaProducer<>(props);
    }

    private RaftClient createRaftClient() {
        RatisServer ratisServer = consumerHandler.getRatisServer();
        if (ratisServer == null) {
            logger.warning("RatisServer not available, operating without consensus");
            return null;
        }

        RaftProperties raftProperties = new RaftProperties();
        return RaftClient.newBuilder()
            .setRaftGroup(ratisServer.getRaftGroup())
            .setClientRpc(new GrpcFactory(new org.apache.ratis.conf.Parameters())
                .newRaftClientRpc(ClientId.randomId(), raftProperties))
            .setProperties(raftProperties)
            .build();
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
            listTopicsResult.names().whenComplete((names, f) -> {
                if (f != null) {
                    f.printStackTrace();
                }
                if (names.stream().noneMatch(name -> name.equals(topic))) {
                    logger.info(String.format("kafka topic not found for %s", topic));
                    final NewTopic newTopic = new NewTopic(topic, settings.getPartitions(), (short) settings.getReplicas());
                    newTopic.configs(new TreeMap<>() {{
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
                } else {
                    countDownLatch.countDown();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            countDownLatch.await(60, TimeUnit.SECONDS);
            CountDownLatch countDownLatchCreatedCheck = new CountDownLatch(1);
            ListTopicsResult listTopicsResult = client.listTopics();
            listTopicsResult.names().whenComplete((names, f) -> {
                if (f != null) {
                    f.printStackTrace();
                    logger.severe(f.getMessage());
                }
                if (names.stream().noneMatch(name -> name.equals(topic))) {
                    logger.severe("topic not created " + topic);
                }
                countDownLatchCreatedCheck.countDown();
            });
            countDownLatchCreatedCheck.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        client.close();
    }

    @Override
    public void push(String key, long version, Modify modify, Callback callback) {
        new Thread(() -> {
            try {
                String serialized = modify.getClass().getSimpleName() +
                    Serializer.getObjectMapper().getOm().writeValueAsString(modify);

                // Submit through Ratis consensus first
                boolean consensusAchieved = submitToRatis(key, serialized);

                // Then publish to Kafka for durable storage and distribution
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    super.getTopic(),
                    key,
                    serialized
                );
                record.headers().add("version", Long.valueOf(version).toString().getBytes());
                record.headers().add("ratis-committed", String.valueOf(consensusAchieved).getBytes());

                producer.send(record, (e, ex) -> {
                    if (ex != null) {
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    if (callback != null) callback.onCompletion(e, ex);
                });
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Override
    public void push(String key, String message) {
        try {
            // Submit through Ratis consensus first
            submitToRatis(key, message);

            // Then publish to Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(
                super.getTopic(),
                key,
                message
            );
            producer.send(record, (e, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Submits an operation to the Ratis leader for consensus.
     * The message format uses null byte as separator between key and value.
     *
     * @return true if consensus was achieved, false if Ratis is unavailable
     */
    private boolean submitToRatis(String key, String value) {
        if (raftClient == null) {
            return false;
        }

        try {
            String combined = key + '\0' + value;
            RaftClientReply reply = raftClient.io().send(
                Message.valueOf(combined)
            );
            if (!reply.isSuccess()) {
                logger.warning("Ratis consensus failed for key=" + key +
                    " exception=" + reply.getException());
                return false;
            }
            return true;
        } catch (Exception e) {
            logger.warning("Ratis submission failed, falling back to Kafka-only: " + e.getMessage());
            return false;
        }
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return producer;
    }

    public RaftClient getRaftClient() {
        return raftClient;
    }
}
