package io.github.prathmesh717.orderservice.config;

import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import io.github.prathmesh717.contracts.event.PaymentRefundedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    private Map<String, Object> commonConfigs() {

        Map<String, Object> configs = new HashMap<>();

        configs.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        configs.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId);

        configs.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        return configs;
    }

    @Bean
    public ConsumerFactory<String, InventoryReservedEvent>
    inventoryReservedConsumerFactory() {

        JsonDeserializer<InventoryReservedEvent> deserializer =
                new JsonDeserializer<>(
                        InventoryReservedEvent.class);

        deserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                commonConfigs(),
                new StringDeserializer(),
                deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            InventoryReservedEvent>
    inventoryReservedKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                InventoryReservedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                inventoryReservedConsumerFactory());

        return factory;
    }

    @Bean
    public ConsumerFactory<String, PaymentRefundedEvent>
    paymentRefundedConsumerFactory() {

        JsonDeserializer<PaymentRefundedEvent> deserializer =
                new JsonDeserializer<>(
                        PaymentRefundedEvent.class);

        deserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                commonConfigs(),
                new StringDeserializer(),
                deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            PaymentRefundedEvent>
    paymentRefundedKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<
                String,
                PaymentRefundedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                paymentRefundedConsumerFactory());

        return factory;
    }
}