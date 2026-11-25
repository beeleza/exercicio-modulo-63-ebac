package com.beeleza.order_service.controller;

import com.beeleza.order_service.dto.OrderCreatedEvent;
import com.beeleza.order_service.dto.OrderCreatedEvent.OrderItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final String topic;

    public OrderController(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
                           @Value("${app.kafka.topics.order-created}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderCreatedEvent criarPedido() {
        // mock de dados simples
        List<OrderItem> items = List.of(
                new OrderItem("PROD-1", 2, new BigDecimal("49.90")),
                new OrderItem("PROD-2", 1, new BigDecimal("120.00"))
        );
        BigDecimal total = items.stream()
                .map(i -> i.price().multiply(BigDecimal.valueOf(i.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                "CUSTOMER-1",
                items,
                total,
                Instant.now()
        );

        // publicar no kafka (orderId como chave garante ordem por pedido na mesma partição)
        kafkaTemplate.send(topic, event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Falha ao publicar pedido {} no tópico {}", event.orderId(), topic, ex);
                    } else {
                        log.info("Pedido {} publicado em {}-{} offset {}", event.orderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

        return event;
    }
}
