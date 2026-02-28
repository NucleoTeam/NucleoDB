package com.nucleodb.library.mqs.kafka.ratis;

import com.nucleodb.library.mqs.config.MQSConfiguration;
import com.nucleodb.library.mqs.config.MQSConstructorSettings;
import com.nucleodb.library.mqs.config.MQSSettings;

/**
 * MQS configuration for the Ratis+Kafka implementation.
 * Combines Apache Ratis consensus with Kafka as the communication fabric.
 *
 * Usage:
 * <pre>
 * RatisConfig ratisConfig = new RatisConfig("n0", 9860, new File("/tmp/ratis"))
 *     .addPeer("n0", "127.0.0.1:9860")
 *     .addPeer("n1", "127.0.0.1:9861")
 *     .addPeer("n2", "127.0.0.1:9862");
 *
 * NucleoDB db = new NucleoDB(
 *     NucleoDB.DBType.ALL,
 *     c -> {
 *         c.getConnectionConfig().setMqsConfiguration(new RatisKafkaConfiguration());
 *         c.getConnectionConfig().getSettingsMap().put("ratisConfig", ratisConfig);
 *     },
 *     c -> {
 *         c.getDataTableConfig().setMqsConfiguration(new RatisKafkaConfiguration());
 *         c.getDataTableConfig().getSettingsMap().put("ratisConfig", ratisConfig);
 *     },
 *     c -> {
 *         c.setMqsConfiguration(new RatisKafkaConfiguration());
 *         c.getSettingsMap().put("ratisConfig", ratisConfig);
 *     },
 *     "com.example.models"
 * );
 * </pre>
 */
public class RatisKafkaConfiguration extends MQSConfiguration {

    public RatisKafkaConfiguration() {
        super(
            new MQSConstructorSettings<>(
                RatisKafkaConsumerHandler.class,
                new String[]{"servers", "groupName", "ratisConfig"},
                new Class[]{MQSSettings.class, String.class, String.class, RatisConfig.class}
            ),
            new MQSConstructorSettings<>(
                RatisKafkaProducerHandler.class,
                new String[]{"servers", "consumerHandler"},
                new Class[]{MQSSettings.class, String.class, RatisKafkaConsumerHandler.class}
            ),
            RatisKafkaSettings.class
        );
    }
}
