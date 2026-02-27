package com.nucleodb.library.mqs.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nucleodb.library.database.index.trie.Entry;
import com.nucleodb.library.database.tables.connection.ConnectionHandler;
import com.nucleodb.library.database.tables.table.DataTable;
import com.nucleodb.library.database.utils.Serializer;
import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.config.MQSSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.*;

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

public class KafkaConsumerHandler extends ConsumerHandler {
    private static Logger logger = Logger.getLogger(KafkaConsumerHandler.class.getName());

    private KafkaConsumer consumer = null;
    private ConnectionHandler connectionHandler = null;

    private ExecutorService thread = Executors.newFixedThreadPool(5);
    private String groupName;
    private String servers;
    private java.util.function.Consumer<Map<Integer, Long>> completeCallback;

    private int threads = 36;

    public KafkaConsumerHandler(MQSSettings settings, String servers, String groupName) {
        super(settings);

        createTopics();

        this.servers = servers;
        this.groupName = groupName;
        logger.info(servers + " using group id " + groupName);
        this.consumer = createConsumer(servers, groupName);

    }
    public KafkaConsumerHandler(KafkaConsumerHandler kafkaConsumerHandler) {
        super(kafkaConsumerHandler.getSettings());
        this.servers = kafkaConsumerHandler.getServers();
        this.groupName = UUID.randomUUID().toString();
        super.setDatabase(kafkaConsumerHandler.getDatabase());
        super.setConnectionHandler(kafkaConsumerHandler.getConnectionHandler());
        super.setQueue(kafkaConsumerHandler.getQueue());
        super.setTopic(kafkaConsumerHandler.getTopic());
        super.setLockManager(kafkaConsumerHandler.getLockManager());
        logger.info(servers + " using group id " + groupName);
        this.consumer = createConsumer(servers, groupName);

    }

    public KafkaConsumerHandler reload(java.util.function.Consumer completeCallback) {
        KafkaConsumerHandler kafkaConsumerHandler = new KafkaConsumerHandler(this);
        kafkaConsumerHandler.setReloadConsumer(true);
        this.completeCallback = completeCallback;
        return kafkaConsumerHandler;
    }

    private KafkaConsumer createConsumer(String bootstrap, String groupName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        //System.out.println(groupName);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ((KafkaSettings) getSettings()).getOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer consumer = new KafkaConsumer(props);

        return consumer;
    }

    public void subscribe(String[] topics) {
        //System.out.println("Subscribed to topic " + Arrays.asList(topics).toString());
        consumer.subscribe(Arrays.asList(topics), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                logger.log(Level.FINEST,"revoked: " + collection.stream().map(c -> c.topic() + c.partition()).collect(Collectors.joining(", ")));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                assigned = collection.stream().map(c -> c.toString()).collect(Collectors.toSet());
                logger.log(Level.FINEST,"assigned: " + assigned.stream().collect(Collectors.joining(", ")));

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
            if (names.stream().filter(name -> name.equals(topic)).count() == 0) {
                logger.log(Level.FINEST,String.format("kafka topic not found for %s", topic));
                final NewTopic newTopic = new NewTopic(topic, ((KafkaSettings) this.getSettings()).getPartitions(), (short) ((KafkaSettings) this.getSettings()).getReplicas());
                newTopic.configs(new TreeMap<>() {{
                    put(TopicConfig.RETENTION_MS_CONFIG, "-1");
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
                if (names.stream().filter(name -> name.equals(topic)).count() == 0) {
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
    }

    private Map<TopicPartition, Long> startupMap = null;

    private boolean initialLoad() {
        Set<TopicPartition> partitions = getConsumer().assignment();
        if (startupMap == null || partitions.size() > startupMap.size()) {
            if (partitions == null) return false;
            if (partitions.size() != threads) return false;
            Map<TopicPartition, Long> tmp = getConsumer().endOffsets(partitions);
            startupMap = tmp;
        }
        return startupMap.size() == startupMap.entrySet().stream().filter(s -> getConsumer().position(s.getKey()) >= s.getValue()).count();
    }

    public void seek(Map<Integer, Long> offsetMap) {
        if (offsetMap.size() > 0) {
            offsetMap.entrySet().forEach(e -> {
                TopicPartition tp = new TopicPartition(this.getSettings().getTable().toLowerCase(), e.getKey());
                logger.log(Level.FINEST,tp + " = " + e.getValue());
                consumer.seek(tp, e.getValue());
            });
        }
    }

    Set<String> assigned = new HashSet<>();


    private void regularConsumer(){
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
                        getConsumer().poll(Duration.ofMillis(100));
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
                seek(offsets);
                super.setStartupLoadCount(getDatabase().getStartupLoadCount());
            } else if (connectionType) {
                offsets = getConnectionHandler().getPartitionOffsets();
                while (assigned.size() < offsets.size()) {
                    try {
                        getConsumer().poll(Duration.ofMillis(100));
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                }
                seek(offsets);
                super.setStartupLoadCount(getConnectionHandler().getStartupLoadCount());
            } else if (lockManagerType) {
                offsets = new HashMap<>();
                super.setStartupLoadCount(new AtomicInteger(0));
            }
        }catch (Exception e){}
        Map<Integer, OffsetAndMetadata> offsetMetaMap = new HashMap<>();
        try {
            do {
                ConsumerRecords<String, String> rs = getConsumer().poll(Duration.ofMillis(1000));
                if (rs.count() > 0) {
                    Map<Integer, Long> finalOffsets = offsets;
                    rs.iterator().forEachRemaining(action -> {
                        Long offsetAtPartition = finalOffsets.get(action.partition());
                        if (offsetAtPartition != null && action.offset() <= offsetAtPartition) return;
                        if (getStartupPhaseConsume().get()) getStartupLoadCount().incrementAndGet();
                        String pop = action.value();
                        //System.out.println("Change added to queue.");
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
                        offsetMetaMap.put(action.partition(), new OffsetAndMetadata(action.offset()));
                    });
                    consumer.commitAsync();
                }

                while (getStartupPhaseConsume().get() && getLeftToRead().get() > 50000) {
                    Thread.sleep(1000);
                }
                //logger.info("consumed: "+leftToRead.get());
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
            logger.log(Level.FINEST, "Consumer interrupted " + (databaseType ? this.getDatabase().getConfig().getTable() : "connections"));
        } catch (Exception e) {
            //e.printStackTrace();
        }
    }

    private void reloadConsumer(){

        boolean connectionType = this.getConnectionHandler() != null;
        boolean databaseType = this.getDatabase() != null;
        boolean lockManagerType = this.getLockManager() != null;

        int partitions = getConsumer().partitionsFor(getTopic()).size();
        while (getConsumer().assignment().size()<partitions) {
            try {
                getConsumer().poll(Duration.ofMillis(100));
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        getConsumer().seekToBeginning(getConsumer().assignment());


        Set<TopicPartition> assignments = getConsumer().assignment();
        Map<Integer, Long> currentOffsets = new HashMap<>();
        Map<Integer, Long> endOffsets = new HashMap<>();
        Map<TopicPartition, Long> endOffsetMap = getConsumer().endOffsets(assignments);
        for (Map.Entry<TopicPartition, Long> entry : endOffsetMap.entrySet()) {
            endOffsets.put(entry.getKey().partition(), entry.getValue());
        }
        boolean completedLoad = false;
        try {
            do {
                ConsumerRecords<String, String> rs = getConsumer().poll(Duration.ofMillis(1000));
                if (rs.count() > 0) {
                    for (ConsumerRecord<String, String> action : rs) {

                        if(endOffsets.get(action.partition())<action.offset()){
                            continue;
                        }
                        currentOffsets.put(action.partition(), action.offset());
                        String pop = action.value();
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
                    }
                    consumer.commitAsync();
                }
                completedLoad = true;
                for (Map.Entry<Integer, Long> integerLongEntry : endOffsets.entrySet()) {
                    if(currentOffsets.get(integerLongEntry.getKey())>=integerLongEntry.getValue()) completedLoad=false;
                }
            } while (!Thread.interrupted() && !completedLoad);
        } catch (Exception e) {
            //e.printStackTrace();
        }
        completeCallback.accept(currentOffsets);
    }

    @Override
    public void run() {

        this.subscribe(new String[]{this.getSettings().getTable().toLowerCase()});

        if(isReloadConsumer()){
            reloadConsumer();
            return;
        }

        regularConsumer();
    }

    public KafkaConsumer getConsumer() {
        return this.consumer;
    }

    public void setConsumer(KafkaConsumer consumer) {
        this.consumer = consumer;
    }

    public ConnectionHandler getConnectionHandler() {
        return connectionHandler;
    }

    public void setConnectionHandler(ConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }
}