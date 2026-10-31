package io.github.prathmesh717.inventoryservice.service;

import io.github.prathmesh717.contracts.event.PaymentSucceededEvent;

public interface InventoryService {

    void reserveInventory(PaymentSucceededEvent event);
}