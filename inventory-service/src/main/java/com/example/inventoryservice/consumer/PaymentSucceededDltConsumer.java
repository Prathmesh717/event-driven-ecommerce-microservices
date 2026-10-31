package io.github.prathmesh717.inventoryservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.PaymentSucceededEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PaymentSucceededDltConsumer {

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_SUCCEEDED_DLT,
            groupId = "inventory-service-dlt-group",
            containerFactory =
                    "kafkaListenerContainerFactory"
    )
    public void consume(
            PaymentSucceededEvent event) {

        log.error(
                """
                DLT EVENT RECEIVED

                eventId={}
                orderId={}
                productId={}
                retryCount={}
                """,
                event.eventId(),
                event.orderId(),
                event.productId(),
                event.retryCount());
    }
}