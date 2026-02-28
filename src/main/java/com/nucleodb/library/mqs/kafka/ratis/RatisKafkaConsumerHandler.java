package com.nucleodb.library.mqs.kafka.ratis;

import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.kafka.KafkaSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Consumer handler that uses Kafka as the communication fabric with Ratis consensus.
 * Messages consumed from Kafka are first committed through the Ratis state machine
 * before being applied to NucleoDB data structures.
 *
 * The Ratis layer ensures all nodes in the cluster agree on the ordering of operations,
 * while Kafka provides the reliable message transport between nodes.
 */
public class RatisKafkaConsumerHandler extends ConsumerHandler {
    private static final Logger logger = Logger.getLogger(RatisKafkaConsumerHandler.class.getName());

    private KafkaConsumer<String, String> consumer = null;
    private final ExecutorService thread = Executors.newFixedThreadPool(5);
    private final String groupName;
    private final String servers;
    private RatisServer ratisServer;
    private int threads = 36;
    Set<String> assigned = new HashSet<>();

    public RatisKafkaConsumerHandler(MQSSettings settings, String servers, String groupName, RatisConfig ratisConfig) {
        super(settings);
        this.servers = servers;
        this.groupName = groupName;

        createTopics();

        logger.info("RatisKafka consumer: " + servers + " using group id " + groupName);
        this.consumer = createConsumer(servers, groupName);

        try {
            this.ratisServer = new RatisServer(ratisConfig);
            this.ratisServer.start();
            logger.info("Ratis server started for consumer");
        } catch (IOException e) {
            logger.severe("Failed to start Ratis server: " + e.getMessage());
            throw new RuntimeException("Failed to start Ratis server", e);
        }
    }

    private KafkaConsumer<String, String> createConsumer(String bootstrap, String groupName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ((KafkaSettings) getSettings()).getOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public void subscribe(String[] topics) {
        consumer.subscribe(Arrays.asList(topics), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                logger.log(Level.FINEST, "revoked: " + collection.stream()
                    .map(c -> c.topic() + c.partition()).collect(Collectors.joining(", ")));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                assigned = collection.stream().map(TopicPartition::toString).collect(Collectors.toSet());
                logger.log(Level.FINEST, "assigned: " + String.join(", ", assigned));
            }
        });
    }

    public void createTopics() {
        Properties props = new Properties();
        KafkaSettings settings = (KafkaSettings) getSettings();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getServers());
        AdminClient client = KafkaAdminClient.create(props);

        String topic = getSettings().getTable().toLowerCase();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ListTopicsResult listTopicsResult = client.listTopics();
            Set<String> names = listTopicsResult.names().get(500, TimeUnit.MILLISECONDS);
            if (names.stream().noneMatch(name -> name.equals(topic))) {
                logger.log(Level.FINEST, String.format("kafka topic not found for %s", topic));
                final NewTopic newTopic = new NewTopic(topic, settings.getPartitions(), (short) settings.getReplicas());
                newTopic.configs(new TreeMap<>() {{
                    put(TopicConfig.RETENTION_MS_CONFIG, "-1");
                    put(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
                }});
                CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
                createTopicsResult.all().whenComplete((c, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
            } else {
                countDownLatch.countDown();
            }
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
    public void start(int queueHandlers) {
        this.threads = queueHandlers;
        thread.submit(new Thread(this));
        super.start(queueHandlers);

        // Start processing committed entries from Ratis state machine
        thread.submit(this::processCommittedEntries);
    }

    /**
     * Background thread that polls committed entries from the Ratis state machine
     * and feeds them into the internal consumer queue for processing by QueueHandler.
     */
    private void processCommittedEntries() {
        boolean connectionType = this.getConnectionHandler() != null;
        boolean databaseType = this.getDatabase() != null;
        boolean lockManagerType = this.getLockManager() != null;

        while (!Thread.interrupted()) {
            NucleoDBStateMachine.CommittedEntry entry;
            while ((entry = ratisServer.getStateMachine().getCommittedEntries().poll()) != null) {
                String value = entry.getValue();
                String key = entry.getKey();

                if (connectionType) {
                    if (this.getConnectionHandler().getConfig().getShardFilter().accept(key)) {
                        getQueue().add(value);
                        getLeftToRead().incrementAndGet();
                        synchronized (getQueue()) {
                            getQueue().notifyAll();
                        }
                    }
                }
                if (databaseType) {
                    if (this.getDatabase().getConfig().getShardFilter().accept(key)) {
                        getQueue().add(value);
                        getLeftToRead().incrementAndGet();
                        synchronized (getQueue()) {
                            getQueue().notifyAll();
                        }
                    }
                }
                if (lockManagerType) {
                    getQueue().add(value);
                    getLeftToRead().incrementAndGet();
                    synchronized (getQueue()) {
                        getQueue().notifyAll();
                    }
                }
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private Map<TopicPartition, Long> startupMap = null;

    private boolean initialLoad() {
        Set<TopicPartition> partitions = consumer.assignment();
        if (startupMap == null || partitions.size() > startupMap.size()) {
            if (partitions == null) return false;
            if (partitions.size() != threads) return false;
            Map<TopicPartition, Long> tmp = consumer.endOffsets(partitions);
            startupMap = tmp;
        }
        return startupMap.size() == startupMap.entrySet().stream()
            .filter(s -> consumer.position(s.getKey()) >= s.getValue()).count();
    }

    public void seek(Map<Integer, Long> offsetMap) {
        if (offsetMap.size() > 0) {
            offsetMap.forEach((partition, offset) -> {
                TopicPartition tp = new TopicPartition(this.getSettings().getTable().toLowerCase(), partition);
                logger.log(Level.FINEST, tp + " = " + offset);
                consumer.seek(tp, offset);
            });
        }
    }

    @Override
    public void run() {
        this.subscribe(new String[]{this.getSettings().getTable().toLowerCase()});
        regularConsumer();
    }

    private void regularConsumer() {
        boolean connectionType = this.getConnectionHandler() != null;
        boolean databaseType = this.getDatabase() != null;
        boolean lockManagerType = this.getLockManager() != null;
        boolean saveConnection = connectionType && this.getConnectionHandler().getConfig().isSaveChanges();
        boolean saveDatabase = databaseType && this.getDatabase().getConfig().isSaveChanges();
        Map<Integer, Long> offsets = new HashMap<>();

        try {
            if (databaseType) {
                offsets = getDatabase().getPartitionOffsets();
                while (assigned.size() < offsets.size()) {
                    try {
                        consumer.poll(Duration.ofMillis(100));
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignored
                    }
                }
                seek(offsets);
                super.setStartupLoadCount(getDatabase().getStartupLoadCount());
            } else if (connectionType) {
                offsets = getConnectionHandler().getPartitionOffsets();
                while (assigned.size() < offsets.size()) {
                    try {
                        consumer.poll(Duration.ofMillis(100));
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignored
                    }
                }
                seek(offsets);
                super.setStartupLoadCount(getConnectionHandler().getStartupLoadCount());
            } else if (lockManagerType) {
                offsets = new HashMap<>();
                super.setStartupLoadCount(new AtomicInteger(0));
            }
        } catch (Exception e) {
            // ignored
        }

        try {
            do {
                ConsumerRecords<String, String> rs = consumer.poll(Duration.ofMillis(1000));
                if (rs.count() > 0) {
                    Map<Integer, Long> finalOffsets = offsets;
                    rs.iterator().forEachRemaining(action -> {
                        Long offsetAtPartition = finalOffsets.get(action.partition());
                        if (offsetAtPartition != null && action.offset() <= offsetAtPartition) return;
                        if (getStartupPhaseConsume().get()) getStartupLoadCount().incrementAndGet();

                        String pop = action.value();

                        // Route through shard filter and add to queue
                        if (connectionType) {
                            if (this.getConnectionHandler().getConfig().getShardFilter().accept(action.key())) {
                                getQueue().add(pop);
                                getLeftToRead().incrementAndGet();
                                synchronized (getQueue()) {
                                    getQueue().notifyAll();
                                }
                            }
                        }
                        if (databaseType) {
                            if (this.getDatabase().getConfig().getShardFilter().accept(action.key())) {
                                getQueue().add(pop);
                                getLeftToRead().incrementAndGet();
                                synchronized (getQueue()) {
                                    getQueue().notifyAll();
                                }
                            }
                        }
                        if (lockManagerType) {
                            getQueue().add(pop);
                            getLeftToRead().incrementAndGet();
                            synchronized (getQueue()) {
                                getQueue().notifyAll();
                            }
                        }

                        if (saveConnection)
                            this.getConnectionHandler().getPartitionOffsets().put(action.partition(), action.offset());
                        if (saveDatabase)
                            this.getDatabase().getPartitionOffsets().put(action.partition(), action.offset());
                    });
                    consumer.commitAsync();
                }

                while (getStartupPhaseConsume().get() && getLeftToRead().get() > 50000) {
                    Thread.sleep(1000);
                }
                if (getStartupPhaseConsume().get() && initialLoad()) {
                    getStartupPhaseConsume().set(false);
                    if (getStartupLoadCount().get() == 0) {
                        if (connectionType) {
                            getConnectionHandler().getStartupPhase().set(false);
                            new Thread(() -> getConnectionHandler().startup()).start();
                        }
                        if (databaseType) {
                            getDatabase().getStartupPhase().set(false);
                            new Thread(() -> getDatabase().startup()).start();
                        }
                        if (lockManagerType) {
                            new Thread(() -> getLockManager().startup()).start();
                        }
                    }
                }
            } while (!Thread.interrupted());
        } catch (Exception e) {
            // ignored
        }
    }

    public RatisServer getRatisServer() {
        return ratisServer;
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return consumer;
    }

    public String getServers() {
        return servers;
    }

    public String getGroupName() {
        return groupName;
    }
}
