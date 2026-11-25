package com.beeleza.notification_service.service;

import com.beeleza.notification_service.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    public void notificar(OrderCreatedEvent pedido) {
        log.info("📧 Enviando e-mail para cliente {}: seu pedido {} de R$ {} foi recebido!",
                pedido.customerId(), pedido.orderId(), pedido.total());
    }
}
