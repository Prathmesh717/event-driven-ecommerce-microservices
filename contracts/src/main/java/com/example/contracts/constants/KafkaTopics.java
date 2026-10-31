package io.github.prathmesh717.contracts.constants;

public final class KafkaTopics {

    private KafkaTopics() {
    }

    public static final String ORDER_CREATED =
            "order-created";

    public static final String PAYMENT_SUCCEEDED =
            "payment-succeeded";

    public static final String INVENTORY_RESERVED =
            "inventory-reserved";

    public static final String INVENTORY_FAILED =
            "inventory-failed";

    public static final String PAYMENT_REFUNDED =
            "payment-refunded";

    // Retry Topics

    public static final String PAYMENT_SUCCEEDED_RETRY =
            "payment-succeeded-retry";

    public static final String PAYMENT_SUCCEEDED_DLT =
            "payment-succeeded-dlt";
}