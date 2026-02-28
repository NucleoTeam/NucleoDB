package com.nucleodb.library.mqs.kafka.ratis;

import com.nucleodb.library.mqs.config.MQSConstructorSettings;
import com.nucleodb.library.mqs.config.MQSSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RatisKafkaConfigurationTest {

    private RatisKafkaConfiguration config;

    @BeforeEach
    public void setup() {
        config = new RatisKafkaConfiguration();
    }

    @Test
    public void consumerHandlerClassIsRatisKafka() {
        MQSConstructorSettings<?> consumer = config.getConsumer();
        assertEquals(RatisKafkaConsumerHandler.class, consumer.getClazz());
    }

    @Test
    public void producerHandlerClassIsRatisKafka() {
        MQSConstructorSettings<?> producer = config.getProducer();
        assertEquals(RatisKafkaProducerHandler.class, producer.getClazz());
    }

    @Test
    public void consumerConstructorGetterElements() {
        String[] elements = config.getConsumer().getConstructorGetterElements();
        assertArrayEquals(new String[]{"servers", "groupName", "ratisConfig"}, elements);
    }

    @Test
    public void producerConstructorGetterElements() {
        String[] elements = config.getProducer().getConstructorGetterElements();
        assertArrayEquals(new String[]{"servers", "consumerHandler"}, elements);
    }

    @Test
    public void consumerConstructorTypes() {
        Class<?>[] types = config.getConsumer().getConstructorTypes();
        assertArrayEquals(
            new Class[]{MQSSettings.class, String.class, String.class, RatisConfig.class},
            types
        );
    }

    @Test
    public void producerConstructorTypes() {
        Class<?>[] types = config.getProducer().getConstructorTypes();
        assertArrayEquals(
            new Class[]{MQSSettings.class, String.class, RatisKafkaConsumerHandler.class},
            types
        );
    }
}
