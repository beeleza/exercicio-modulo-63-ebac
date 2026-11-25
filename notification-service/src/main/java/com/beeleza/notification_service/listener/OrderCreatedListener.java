package com.beeleza.notification_service.listener;

import com.beeleza.notification_service.event.OrderCreatedEvent;
import com.beeleza.notification_service.service.NotificationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final NotificationService notificationService;

    public OrderCreatedListener(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = "${app.kafka.topics.order-created}")
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record) {
        OrderCreatedEvent pedido = record.value();

        log.info("Pedido recebido {} | partição={} offset={}",
                pedido.orderId(), record.partition(), record.offset());

        notificationService.notificar(pedido);
    }
}
