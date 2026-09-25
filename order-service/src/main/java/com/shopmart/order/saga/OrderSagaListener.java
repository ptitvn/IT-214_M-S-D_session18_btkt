package com.shopmart.order.saga;

import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu 3: order-service lắng nghe kết quả các bước Saga do inventory-service / payment-service phát ra
 * để cập nhật trạng thái cuối cùng của đơn hàng.
 *
 * - INVENTORY_FAILED  : hết hàng -> huỷ đơn ngay (chưa có bước nào khác cần hoàn tác).
 * - PAYMENT_COMPLETED : mọi bước đều thành công -> hoàn tất đơn hàng.
 * - PAYMENT_FAILED    : thanh toán lỗi -> huỷ đơn (inventory-service sẽ tự hoàn tồn kho song song).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaListener {

    private final OrderService orderService;

    @KafkaListener(topics = "order", groupId = "${spring.kafka.consumer.group-id:order-service-group}")
    public void onOrderEvent(OrderEvent event) {
        log.info("[Kafka][order-service] <- received {} orderId={}", event.getType(), event.getOrderId());

        switch (event.getType()) {
            case INVENTORY_FAILED -> {
                log.error("[Saga] orderId={} CANCELLED - lý do: {}", event.getOrderId(), event.getMessage());
                orderService.cancelOrder(event.getOrderId(), "Hết hàng: " + event.getMessage());
            }
            case PAYMENT_COMPLETED -> {
                log.info("[Saga] orderId={} COMPLETED - toàn bộ các bước đã thành công", event.getOrderId());
                orderService.completeOrder(event.getOrderId());
            }
            case PAYMENT_FAILED -> {
                log.error("[Saga] orderId={} CANCELLED - thanh toán thất bại: {}", event.getOrderId(), event.getMessage());
                orderService.cancelOrder(event.getOrderId(), "Thanh toán thất bại: " + event.getMessage());
            }
            default -> {
                // ORDER_CREATED do chính order-service phát ra, INVENTORY_RESERVED và
                // INVENTORY_RELEASED không cần order-service xử lý -> bỏ qua.
            }
        }
    }
}
