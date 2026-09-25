package com.shopmart.order.client;

/** Ném ra khi Circuit Breaker OPEN hoặc lệnh gọi inventory-service thất bại và fallback được kích hoạt. */
public class InventoryServiceUnavailableException extends RuntimeException {

    public InventoryServiceUnavailableException(String message) {
        super(message);
    }
}
