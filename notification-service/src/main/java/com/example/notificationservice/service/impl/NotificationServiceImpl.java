package io.github.prathmesh717.notificationservice.service.impl;

import io.github.prathmesh717.contracts.event.InventoryReservedEvent;
import io.github.prathmesh717.contracts.event.PaymentRefundedEvent;
import io.github.prathmesh717.notificationservice.service.NotificationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationServiceImpl
        implements NotificationService {

    @Override
    public void sendOrderCompletedNotification(
            InventoryReservedEvent event) {

        log.info(
                """
                EMAIL SENT

                Subject: Order Completed

                Order Id: {}
                Product Id: {}
                Quantity : {}
                """,
                event.orderId(),
                event.productId(),
                event.quantity());
    }

    @Override
    public void sendOrderCancelledNotification(
            PaymentRefundedEvent event) {

        log.info(
                """
                EMAIL SENT

                Subject: Order Cancelled

                Order Id: {}
                Reason   : {}
                """,
                event.orderId(),
                event.reason());
    }
}