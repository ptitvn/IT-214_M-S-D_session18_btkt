package com.shopmart.order.controller;

import com.shopmart.order.client.InventoryClientService;
import com.shopmart.order.dto.OrderRequest;
import com.shopmart.order.dto.OrderResponse;
import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import com.shopmart.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/order")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final InventoryClientService inventoryClientService;

    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody OrderRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(orderService.createOrder(request));
    }

    @GetMapping("/{id}")
    public OrderResponse getById(@PathVariable Long id) {
        return orderService.getOrderById(id);
    }

    @GetMapping
    public List<OrderResponse> getAll() {
        return orderService.getAllOrders();
    }

    /**
     * Câu 2: gọi thử inventory-service qua FeignClient (LoadBalancer + Resilience4j Circuit Breaker).
     * Gọi liên tục endpoint này khi tắt hết instance inventory-service để quan sát circuit breaker
     * chuyển CLOSED -> OPEN -> HALF-OPEN qua GET /actuator/circuitbreakers.
     */
    @GetMapping("/products/{id}")
    public ProductDto getProductViaFeign(@PathVariable Long id) {
        return inventoryClientService.getProduct(id);
    }

    /**
     * Câu 2 (demo độc lập với Saga): trừ tồn kho trực tiếp qua FeignClient + Circuit Breaker,
     * không thông qua Kafka. Dùng để kiểm thử riêng lẻ khả năng chịu lỗi khi inventory-service
     * ngưng hoạt động (fallback trả lỗi 503 thay vì để request bị treo / lỗi 500 dây chuyền).
     */
    @PutMapping("/products/{id}/decrease-test")
    public ProductDto decreaseStockViaFeign(@PathVariable Long id, @RequestBody StockRequest request) {
        return inventoryClientService.decreaseStock(id, request.getQuantity());
    }
}
