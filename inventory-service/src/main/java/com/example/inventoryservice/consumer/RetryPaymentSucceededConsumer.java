package io.github.prathmesh717.inventoryservice.consumer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.PaymentSucceededEvent;
import io.github.prathmesh717.inventoryservice.producer.RetryEventProducer;
import io.github.prathmesh717.inventoryservice.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryPaymentSucceededConsumer {

    private static final int MAX_RETRIES = 3;

    private final InventoryService inventoryService;

    private final RetryEventProducer retryEventProducer;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_SUCCEEDED_RETRY,
            groupId = "inventory-service-retry-group",
            containerFactory =
                    "kafkaListenerContainerFactory"
    )
    public void consume(
            PaymentSucceededEvent event) {

        try {

            // Simple backoff before retrying
            Thread.sleep(5000);

            inventoryService.reserveInventory(
                    event);

            log.info(
                    "Retry successful. orderId={}, retryCount={}",
                    event.orderId(),
                    event.retryCount());

        } catch (Exception ex) {

            log.error(
                    "Retry failed. orderId={}, retryCount={}",
                    event.orderId(),
                    event.retryCount(),
                    ex);

            PaymentSucceededEvent retryEvent =
                    new PaymentSucceededEvent(
                            event.eventId(),
                            event.retryCount() + 1,
                            event.paymentId(),
                            event.orderId(),
                            event.productId(),
                            event.quantity(),
                            event.amount());

            if (retryEvent.retryCount()
                    >= MAX_RETRIES) {

                retryEventProducer
                        .publishDltEvent(
                                retryEvent);

            } else {

                retryEventProducer
                        .publishRetryEvent(
                                retryEvent);
            }
        }
    }
}