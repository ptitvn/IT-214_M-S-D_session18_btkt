# GIẢI PHÁP – ShopMart Microservices (Saga Pattern + Spring Cloud)

Tài liệu này tóm tắt những gì đã được cài đặt cho từng câu, và cách chạy/kiểm thử để demo.

## 0. Chuẩn bị

```bash
# Xoá .git của base project rồi khởi tạo repo riêng (đã làm sẵn trong bản zip này)
git init
git add .
git commit -m "init"

# Khởi động hạ tầng: MySQL + Kafka (KRaft) + Redis
docker compose up -d

# Build toàn bộ
mvn clean install
```

> Nếu máy không kéo được image `apache/kafka:3.7.0` (do lỗi mạng/DNS), có thể đổi image Kafka
> trong `docker-compose.yml` sang bản `confluentinc/cp-kafka` + `confluentinc/cp-zookeeper` truyền
> thống — cấu hình `spring.kafka.bootstrap-servers=localhost:9092` ở `config-repo/application.yml`
> không cần đổi.

## 1. Thứ tự khởi động service (rất quan trọng)

```bash
mvn -pl config-server spring-boot:run     # 1) 8888 - phải chạy trước tiên
mvn -pl eureka-server spring-boot:run     # 2) 8761
mvn -pl api-gateway   spring-boot:run     # 3) 8080
mvn -pl inventory-service spring-boot:run # 4) 8082
mvn -pl payment-service   spring-boot:run # 5) 8083
mvn -pl order-service     spring-boot:run # 6) 8081
```

Kiểm tra:
- Config Server: `http://localhost:8888/order-service/default`
- Eureka Dashboard: `http://localhost:8761` (phải thấy đủ 4 service: order/inventory/payment/api-gateway)
- Gọi qua Gateway: `http://localhost:8080/api/inventory/products`

## 2. Câu 1 – Config Server / Eureka / Gateway

- `config-server`: dùng **native profile**, đọc file trong `config-repo/` (không cần Git). Mỗi
  service khai báo `spring.config.import=optional:configserver:http://localhost:8888` — nếu
  Config Server chưa chạy, service vẫn khởi động được nhờ `optional:` + `fail-fast: false`.
- `config-repo/application.yml`: cấu hình dùng chung (Eureka URL, Kafka, Redis, Actuator).
- `config-repo/<service>.yml`: cấu hình riêng (datasource, port, resilience4j, kafka group-id...).
- `eureka-server`: `@EnableEurekaServer`, tắt `register-with-eureka`/`fetch-registry` cho chính nó.
- `api-gateway`: định tuyến bằng `lb://order-service`, `lb://inventory-service`, `lb://payment-service`
  (LoadBalancer qua Eureka), route `/api/order/**`, `/api/inventory/**`, `/api/payment/**`.

## 3. Câu 2 – FeignClient + Circuit Breaker (Resilience4j)

- `order-service/client/InventoryClient`: `@FeignClient(name = "inventory-service")` với 2
  method: `getProduct` (GET) và `decreaseStock` (PUT) — LoadBalancer tự chọn instance qua Eureka.
- `order-service/client/InventoryClientService`: bọc Feign bằng
  `@CircuitBreaker(name = "inventoryService", fallbackMethod = ...)`. Cấu hình 3 trạng thái ở
  `config-repo/order-service.yml` (`resilience4j.circuitbreaker.instances.inventoryService`).
- Demo Load Balancing: chạy thêm 1 instance inventory-service ở port khác:
  ```bash
  mvn -pl inventory-service spring-boot:run -Dspring-boot.run.arguments=--server.port=8084
  ```
  Gọi liên tục `GET /api/order/products/1` (qua order-service) — log `inventory-service instance
  on port ...` sẽ đổi giữa 8082/8084.
- Demo Circuit Breaker (CLOSED → OPEN → HALF-OPEN): tắt **tất cả** instance inventory-service, gọi
  liên tục `GET http://localhost:8081/api/order/products/1`. Ban đầu vài lần lỗi thật (CLOSED),
  sau khi vượt `failure-rate-threshold` breaker chuyển OPEN (fallback trả 503 ngay, không gọi
  mạng), theo dõi qua `GET http://localhost:8081/actuator/circuitbreakers`. Sau
  `wait-duration-in-open-state` (10s), breaker cho thử lại (HALF-OPEN) — bật lại inventory-service
  để thấy nó về CLOSED.

## 4. Câu 3 – Saga (Choreography) qua Kafka

Thiết kế **Choreography Saga**: mỗi service tự lắng nghe topic `order` và tự quyết định hành động,
không có một "nhạc trưởng" trung tâm.

```
order-service                inventory-service              payment-service
     |--ORDER_CREATED-------------->|                               |
     |                        (trừ tồn kho)                         |
     |<--INVENTORY_FAILED-----------|   (hết hàng, dừng ở đây)       |
     |                               |--INVENTORY_RESERVED--------->|
     |                               |                        (thanh toán)
     |<-------------------------PAYMENT_COMPLETED---------------------|  (thành công)
     |                               |<--------PAYMENT_FAILED----------|  (thất bại)
     |                        (hoàn tồn kho = compensate)             |
     |<--INVENTORY_RELEASED---------|                                |
     |  cancelOrder(...)                                             |
```

- `order-service.OrderServiceImpl.createOrder`: gọi Feign lấy giá → lưu đơn `PENDING` → publish
  `ORDER_CREATED`.
- `inventory-service.InventorySagaListener`: nhận `ORDER_CREATED` → `decreaseStock` → publish
  `INVENTORY_RESERVED` (thành công) hoặc `INVENTORY_FAILED` (hết hàng).
- `payment-service.PaymentSagaListener`: nhận `INVENTORY_RESERVED` → `processPayment` → publish
  `PAYMENT_COMPLETED` hoặc `PAYMENT_FAILED`.
- `inventory-service.InventorySagaListener`: nhận `PAYMENT_FAILED` → **compensating transaction**
  `increaseStock` (hoàn tồn kho) → publish `INVENTORY_RELEASED`.
- `order-service.OrderSagaListener`: nhận `INVENTORY_FAILED`/`PAYMENT_FAILED` → `cancelOrder`;
  nhận `PAYMENT_COMPLETED` → `completeOrder`.

### Chứng minh rollback (bắt buộc)

1. Đặt `payment.simulate-failure: true` trong `config-repo/payment-service.yml` (hoặc gọi đơn có
   `productId` = MacBook (`id=3`, giá 28.000.000) với `quantity=3` → 84.000.000 > hạn mức
   80.000.000 → tự động FAILED mà không cần đổi cấu hình).
2. `POST /api/order` → đơn tạo `PENDING`, tồn kho MacBook giảm 3.
3. Theo dõi log: `payment-service` in `Payment FAILED`, publish `PAYMENT_FAILED`;
   `inventory-service` in `[Saga][Compensate] Restored stock...`; `order-service` in
   `orderId=... CANCELLED`.
4. `GET /api/order/{id}` → status = `CANCELLED`. `GET /api/inventory/products/3` → tồn kho đã
   **về đúng số lượng ban đầu** (không bị lệch dữ liệu).

### Câu 3 (Nâng cao) – Reactive consumer (WebFlux)

`inventory-service.saga.ReactiveOrderEventConsumer` dùng **reactor-kafka** (`KafkaReceiver`,
non-blocking) để lắng nghe song song topic `order` với group riêng
(`inventory-service-reactive-audit`) chỉ để log audit theo kiểu reactive — minh chứng tư duy
Async/Event-driven mà không ảnh hưởng tới luồng nghiệp vụ chính (vẫn dùng `@KafkaListener` chuẩn,
đơn giản, dễ kiểm thử/debug hơn cho phần Saga cốt lõi).

## 5. Câu 4 – Redis Cache-Aside (inventory-service)

- `@Cacheable("products")` trên `getProductById` → log `Querying DB for product id=...` chỉ in ra
  **lần đầu tiên**; gọi lại `GET /api/inventory/products/{id}` các lần sau không thấy log này nữa
  (dữ liệu lấy từ Redis).
- `@CachePut("products")` trên `updateProduct`, `decreaseStock`, `increaseStock` → cache luôn được
  cập nhật ngay bằng dữ liệu mới nhất mỗi khi tồn kho thay đổi (kể cả do Saga trừ/hoàn kho), tránh
  cache bị "stale" (cũ).
- `@CacheEvict("products")` trên `deleteProduct`.
- Kiểm tra trực tiếp trong Redis: `docker exec -it shopmart-redis redis-cli KEYS "products*"`.

## 6. Câu 5 – Clean code / Logging / Test

- Không hard-code cấu hình: mọi giá trị môi trường (port, datasource, kafka, redis, resilience4j)
  đều nằm ở `config-repo/*.yml`, service chỉ giữ `spring.application.name` +
  `spring.config.import`.
- SLF4J: log `INFO` cho luồng thành công, `ERROR` cho lỗi/compensate ở mọi bước Saga và Circuit
  Breaker fallback (dễ dàng demo bằng cách xem console khi chạy kịch bản rollback ở mục 4).
- Unit test:
  - `OrderServiceImplTest`: test tạo đơn (mock Feign + verify publish `ORDER_CREATED`), test
    `cancelOrder`/`completeOrder`, và **test rollback**
    (`cancelOrder_onPaymentFailedRollback_shouldNeverLeaveOrderPendingOrCompleted`).
  - `PaymentServiceImplTest`: test `processPayment` thành công/thất bại, và test `refund()`
    (thành công + ném lỗi khi trạng thái không phải `SUCCESS`).
  - Chạy: `mvn test`.
