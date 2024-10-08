package com.nucleodb.library.mqs.local;

import com.nucleodb.library.mqs.ConsumerHandler;
import com.nucleodb.library.mqs.ProducerHandler;
import com.nucleodb.library.mqs.config.MQSConfiguration;
import com.nucleodb.library.mqs.config.MQSConstructorSettings;
import com.nucleodb.library.mqs.config.MQSSettings;
import com.nucleodb.library.mqs.kafka.KafkaConsumerHandler;
import com.nucleodb.library.mqs.kafka.KafkaProducerHandler;

public class LocalConfiguration extends MQSConfiguration{

  public LocalConfiguration() {
    super(
        new MQSConstructorSettings<>(
            LocalConsumerHandler.class,
            new String[]{},
            new Class[]{MQSSettings.class}
        ),
        new MQSConstructorSettings<>(
            LocalProducerHandler.class,
            new String[]{"consumerHandler"},
            new Class[]{MQSSettings.class, LocalConsumerHandler.class}
        ),
        MQSSettings.class
    );
  }
}
