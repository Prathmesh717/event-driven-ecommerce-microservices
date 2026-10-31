package io.github.prathmesh717.analyticsservice.config;

import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import io.github.prathmesh717.contracts.event.OrderCreatedEvent;
import io.github.prathmesh717.contracts.event.PaymentRefundedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> configs() {

        Map<String, Object> configs =
                new HashMap<>();

        configs.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);

        configs.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        return configs;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            OrderCreatedEvent>
    orderCreatedKafkaListenerContainerFactory() {

        JsonDeserializer<OrderCreatedEvent> deserializer =
                new JsonDeserializer<>(
                        OrderCreatedEvent.class);

        deserializer.addTrustedPackages("*");

        ConcurrentKafkaListenerContainerFactory<
                String,
                OrderCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                new DefaultKafkaConsumerFactory<>(
                        configs(),
                        new StringDeserializer(),
                        deserializer));

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            InventoryReservedEvent>
    inventoryReservedKafkaListenerContainerFactory() {

        JsonDeserializer<InventoryReservedEvent> deserializer =
                new JsonDeserializer<>(
                        InventoryReservedEvent.class);

        deserializer.addTrustedPackages("*");

        ConcurrentKafkaListenerContainerFactory<
                String,
                InventoryReservedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                new DefaultKafkaConsumerFactory<>(
                        configs(),
                        new StringDeserializer(),
                        deserializer));

        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<
            String,
            PaymentRefundedEvent>
    paymentRefundedKafkaListenerContainerFactory() {

        JsonDeserializer<PaymentRefundedEvent> deserializer =
                new JsonDeserializer<>(
                        PaymentRefundedEvent.class);

        deserializer.addTrustedPackages("*");

        ConcurrentKafkaListenerContainerFactory<
                String,
                PaymentRefundedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(
                new DefaultKafkaConsumerFactory<>(
                        configs(),
                        new StringDeserializer(),
                        deserializer));

        return factory;
    }
}