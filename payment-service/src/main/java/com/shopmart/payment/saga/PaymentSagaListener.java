package com.shopmart.payment.saga;

import com.shopmart.payment.dto.PaymentRequest;
import com.shopmart.payment.dto.PaymentResponse;
import com.shopmart.payment.entity.PaymentStatus;
import com.shopmart.payment.event.OrderEvent;
import com.shopmart.payment.event.SagaEventType;
import com.shopmart.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Câu 3: Choreography Saga - payment-service chỉ hành động khi tồn kho đã được trừ thành công
 * (INVENTORY_RESERVED). Kết quả thanh toán quyết định bước tiếp theo của toàn bộ Saga:
 *  - SUCCESS -> publish PAYMENT_COMPLETED  (order-service sẽ COMPLETE đơn hàng)
 *  - FAILED  -> publish PAYMENT_FAILED     (order-service CANCEL đơn hàng,
 *                                           inventory-service compensate hoàn tồn kho)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSagaListener {

    private final PaymentService paymentService;
    private final OrderEventProducer producer;

    @KafkaListener(topics = "order", groupId = "${spring.kafka.consumer.group-id:payment-service-group}")
    public void onOrderEvent(OrderEvent event) {
        log.info("[Kafka][payment-service] <- received {} orderId={}", event.getType(), event.getOrderId());

        if (event.getType() != SagaEventType.INVENTORY_RESERVED) {
            // ORDER_CREATED, INVENTORY_FAILED, PAYMENT_*, INVENTORY_RELEASED không do
            // payment-service xử lý ở bước này -> bỏ qua.
            return;
        }

        PaymentResponse response = paymentService.processPayment(
                new PaymentRequest(event.getOrderId(), event.getAmount()));

        SagaEventType resultType = response.getStatus() == PaymentStatus.SUCCESS
                ? SagaEventType.PAYMENT_COMPLETED
                : SagaEventType.PAYMENT_FAILED;

        producer.publish(OrderEvent.builder()
                .orderId(event.getOrderId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .amount(event.getAmount())
                .type(resultType)
                .message(response.getMessage())
                .build());
    }
}
