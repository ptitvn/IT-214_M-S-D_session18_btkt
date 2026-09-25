package com.shopmart.inventory.saga;

import com.shopmart.inventory.event.OrderEvent;
import com.shopmart.inventory.event.SagaEventType;
import com.shopmart.inventory.exception.InsufficientStockException;
import com.shopmart.inventory.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu 3: Choreography Saga - inventory-service tham gia bằng cách lắng nghe topic "order".
 *
 * - ORDER_CREATED   : thử trừ tồn kho. Thành công -> INVENTORY_RESERVED, thất bại -> INVENTORY_FAILED.
 * - PAYMENT_FAILED  : compensating transaction - hoàn lại tồn kho đã trừ -> INVENTORY_RELEASED.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventorySagaListener {

    private final ProductService productService;
    private final OrderEventProducer producer;

    @KafkaListener(topics = "order", groupId = "${spring.kafka.consumer.group-id:inventory-service-group}")
    public void onOrderEvent(OrderEvent event) {
        log.info("[Kafka][inventory-service] <- received {} orderId={}", event.getType(), event.getOrderId());

        switch (event.getType()) {
            case ORDER_CREATED -> reserveStock(event);
            case PAYMENT_FAILED -> compensateStock(event);
            default -> {
                // Các sự kiện khác (INVENTORY_RESERVED, INVENTORY_FAILED, PAYMENT_COMPLETED,
                // INVENTORY_RELEASED) không do inventory-service xử lý -> bỏ qua.
            }
        }
    }

    private void reserveStock(OrderEvent event) {
        try {
            productService.decreaseStock(event.getProductId(), event.getQuantity());
            log.info("[Saga] Inventory reserved for orderId={} productId={} quantity={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            producer.publish(OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_RESERVED)
                    .message("Đã trừ tồn kho")
                    .build());
        } catch (InsufficientStockException ex) {
            log.error("[Saga] Inventory FAILED for orderId={}: {}", event.getOrderId(), ex.getMessage());
            producer.publish(OrderEvent.builder()
                    .orderId(event.getOrderId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .amount(event.getAmount())
                    .type(SagaEventType.INVENTORY_FAILED)
                    .message(ex.getMessage())
                    .build());
        }
    }

    /** Compensating transaction: thanh toán thất bại -> hoàn lại số lượng đã trừ tồn kho. */
    private void compensateStock(OrderEvent event) {
        productService.increaseStock(event.getProductId(), event.getQuantity());
        log.info("[Saga][Compensate] Restored stock for orderId={} productId={} quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
        producer.publish(OrderEvent.builder()
                .orderId(event.getOrderId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .amount(event.getAmount())
                .type(SagaEventType.INVENTORY_RELEASED)
                .message("Đã hoàn tồn kho do thanh toán thất bại")
                .build());
    }
}
