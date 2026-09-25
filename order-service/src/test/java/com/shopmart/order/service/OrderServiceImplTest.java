package com.shopmart.order.service;

import com.shopmart.order.client.InventoryClientService;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.entity.Order;
import com.shopmart.order.entity.OrderStatus;
import com.shopmart.order.event.OrderEvent;
import com.shopmart.order.event.SagaEventType;
import com.shopmart.order.repository.OrderRepository;
import com.shopmart.order.saga.OrderEventProducer;
import com.shopmart.order.service.impl.OrderServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceImplTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private InventoryClientService inventoryClientService;

    @Mock
    private OrderEventProducer orderEventProducer;

    @InjectMocks
    private OrderServiceImpl orderService;

    @Test
    void createOrder_shouldSaveWithPendingStatus_andPublishOrderCreatedEvent() {
        when(inventoryClientService.getProduct(1L))
                .thenReturn(ProductDto.builder().id(1L).name("MacBook").price(new BigDecimal("28000000")).stock(10).build());
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order order = inv.getArgument(0);
            order.setId(1L);
            return order;
        });

        OrderResponse result = orderService.createOrder(new OrderRequest("C001", 1L, 2));

        assertThat(result.getId()).isEqualTo(1L);
        assertThat(result.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.getTotalAmount()).isEqualByComparingTo("56000000");

        ArgumentCaptor<OrderEvent> captor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(orderEventProducer).publish(captor.capture());
        assertThat(captor.getValue().getType()).isEqualTo(SagaEventType.ORDER_CREATED);
        assertThat(captor.getValue().getOrderId()).isEqualTo(1L);
    }

    @Test
    void cancelOrder_shouldSetCancelledWithReason() {
        Order order = Order.builder().id(1L).status(OrderStatus.PENDING).build();
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse result = orderService.cancelOrder(1L, "Payment failed");

        assertThat(result.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(result.getFailureReason()).isEqualTo("Payment failed");
    }

    @Test
    void completeOrder_shouldSetCompletedStatus() {
        Order order = Order.builder().id(1L).status(OrderStatus.PENDING).build();
        when(orderRepository.findById(1L)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse result = orderService.completeOrder(1L);

        assertThat(result.getStatus()).isEqualTo(OrderStatus.COMPLETED);
    }

    /**
     * Câu 5: Test kịch bản ROLLBACK của Saga - khi payment-service báo PAYMENT_FAILED,
     * OrderSagaListener gọi orderService.cancelOrder(...). Ở mức unit test cho OrderServiceImpl,
     * ta kiểm chứng rằng cancelOrder() luôn đưa đơn hàng về đúng trạng thái CANCELLED kèm lý do
     * thất bại thanh toán, và KHÔNG để đơn hàng ở trạng thái PENDING hay COMPLETED (dữ liệu
     * không bị lệch giữa các service khi Saga rollback).
     */
    @Test
    void cancelOrder_onPaymentFailedRollback_shouldNeverLeaveOrderPendingOrCompleted() {
        Order order = Order.builder().id(2L).status(OrderStatus.PENDING).build();
        when(orderRepository.findById(2L)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

        OrderResponse result = orderService.cancelOrder(2L, "Thanh toán thất bại: Số tiền vượt hạn mức");

        assertThat(result.getStatus())
                .as("Saga rollback phải chuyển đơn hàng sang CANCELLED, không được để PENDING/COMPLETED")
                .isEqualTo(OrderStatus.CANCELLED)
                .isNotEqualTo(OrderStatus.PENDING)
                .isNotEqualTo(OrderStatus.COMPLETED);
        assertThat(result.getFailureReason()).contains("Thanh toán thất bại");
    }
}
