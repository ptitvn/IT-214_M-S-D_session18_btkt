package com.shopmart.order.client;

import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Câu 2: Bọc FeignClient bằng Resilience4j @CircuitBreaker + fallback method.
 *
 * 3 trạng thái của Circuit Breaker "inventoryService" (cấu hình ở config-repo/order-service.yml):
 *  - CLOSED    : hoạt động bình thường, mọi request đều được gọi thật tới inventory-service.
 *  - OPEN      : tỉ lệ lỗi trong cửa sổ trượt (sliding window) vượt failure-rate-threshold ->
 *                circuit breaker "bật", các request tiếp theo bị chặn ngay và trả fallback
 *                (không gọi thật, tránh cascading failure) trong wait-duration-in-open-state.
 *  - HALF_OPEN : sau thời gian chờ, cho phép một số lượng nhỏ request thử lại; nếu thành công ->
 *                chuyển về CLOSED, nếu vẫn lỗi -> quay lại OPEN.
 * Có thể theo dõi trạng thái qua: GET http://localhost:8081/actuator/circuitbreakers
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryClientService {

    private final InventoryClient inventoryClient;

    @CircuitBreaker(name = "inventoryService", fallbackMethod = "getProductFallback")
    public ProductDto getProduct(Long id) {
        return inventoryClient.getProduct(id);
    }

    @CircuitBreaker(name = "inventoryService", fallbackMethod = "decreaseStockFallback")
    public ProductDto decreaseStock(Long id, int quantity) {
        return inventoryClient.decreaseStock(id, new StockRequest(quantity));
    }

    @SuppressWarnings("unused")
    private ProductDto getProductFallback(Long id, Throwable throwable) {
        log.error("[CircuitBreaker][inventoryService] getProduct(id={}) fallback triggered - {}: {}",
                id, throwable.getClass().getSimpleName(), throwable.getMessage());
        throw new InventoryServiceUnavailableException(
                "inventory-service đang không khả dụng (circuit breaker OPEN hoặc lỗi kết nối), thử lại sau");
    }

    @SuppressWarnings("unused")
    private ProductDto decreaseStockFallback(Long id, int quantity, Throwable throwable) {
        log.error("[CircuitBreaker][inventoryService] decreaseStock(id={}, qty={}) fallback triggered - {}: {}",
                id, quantity, throwable.getClass().getSimpleName(), throwable.getMessage());
        throw new InventoryServiceUnavailableException(
                "inventory-service đang không khả dụng (circuit breaker OPEN hoặc lỗi kết nối), thử lại sau");
    }
}
