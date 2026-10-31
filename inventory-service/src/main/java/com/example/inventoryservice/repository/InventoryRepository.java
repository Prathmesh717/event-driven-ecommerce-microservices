package io.github.prathmesh717.inventoryservice.repository;

import io.github.prathmesh717.inventoryservice.entity.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryRepository extends JpaRepository<Inventory, Long> {
}