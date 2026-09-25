package com.shopmart.inventory.saga;

import com.shopmart.inventory.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Câu 3 (Nâng cao): Minh chứng tư duy Async / Event-driven bằng reactor-kafka (WebFlux).
 *
 * Đây là một consumer ĐỘC LẬP (group riêng "inventory-service-reactive-audit") chỉ dùng để
 * ghi log/audit lại toàn bộ dòng sự kiện Saga theo kiểu reactive (non-blocking), song song với
 * InventorySagaListener (spring-kafka, blocking) là consumer chịu trách nhiệm nghiệp vụ chính.
 * Vì group-id khác nhau nên Kafka sẽ phát lại toàn bộ message cho cả hai consumer group.
 */
@Slf4j
@Component
public class ReactiveOrderEventConsumer implements ApplicationRunner {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Override
    public void run(ApplicationArguments args) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-service-reactive-audit");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());

        ReceiverOptions<String, OrderEvent> receiverOptions = ReceiverOptions.<String, OrderEvent>create(props)
                .subscription(Collections.singletonList("order"));

        Flux<OrderEvent> eventFlux = KafkaReceiver.create(receiverOptions)
                .receive()
                .map(record -> {
                    OrderEvent event = record.value();
                    record.receiverOffset().acknowledge();
                    return event;
                });

        eventFlux.subscribe(
                event -> log.info("[ReactiveKafka][WebFlux][inventory-service] audit <- {} orderId={} message={}",
                        event.getType(), event.getOrderId(), event.getMessage()),
                error -> log.error("[ReactiveKafka][inventory-service] error while consuming saga events", error)
        );

        log.info("[ReactiveKafka][inventory-service] reactive audit consumer subscribed to topic 'order'");
    }
}
