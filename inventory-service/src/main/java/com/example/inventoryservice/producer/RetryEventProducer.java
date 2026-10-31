package io.github.prathmesh717.inventoryservice.producer;

import io.github.prathmesh717.contracts.constants.KafkaTopics;
import io.github.prathmesh717.contracts.event.PaymentSucceededEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RetryEventProducer {

    private final KafkaTemplate<String, Object>
            kafkaTemplate;

    public void publishRetryEvent(
            PaymentSucceededEvent event) {

        kafkaTemplate.send(
                KafkaTopics.PAYMENT_SUCCEEDED_RETRY,
                event.orderId().toString(),
                event);

        log.warn(
                "Retry event published. orderId={}, retryCount={}",
                event.orderId(),
                event.retryCount());
    }

    public void publishDltEvent(
            PaymentSucceededEvent event) {

        kafkaTemplate.send(
                KafkaTopics.PAYMENT_SUCCEEDED_DLT,
                event.orderId().toString(),
                event);

        log.error(
                "DLT event published. orderId={}, retryCount={}",
                event.orderId(),
                event.retryCount());
    }
}