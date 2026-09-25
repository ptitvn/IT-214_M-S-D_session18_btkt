package com.shopmart.order.client;

import com.shopmart.order.dto.ProductDto;
import com.shopmart.order.dto.StockRequest;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * Câu 2: FeignClient gọi inventory-service theo TÊN service đã đăng ký trong Eureka
 * (value = "inventory-service"). Spring Cloud LoadBalancer sẽ tự chọn 1 trong các
 * instance đang UP để cân bằng tải (vd 2 instance ở port 8082 và 8084).
 */
@FeignClient(name = "inventory-service")
public interface InventoryClient {

    @GetMapping("/api/inventory/products/{id}")
    ProductDto getProduct(@PathVariable("id") Long id);

    @PutMapping("/api/inventory/products/{id}/decrease")
    ProductDto decreaseStock(@PathVariable("id") Long id, @RequestBody StockRequest request);
}
