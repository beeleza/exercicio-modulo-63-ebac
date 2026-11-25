package com.beeleza.notification_service;

import com.beeleza.notification_service.event.OrderCreatedEvent;
import com.beeleza.notification_service.service.NotificationService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = "order.created")
class NotificationServiceApplicationTests {

	@Autowired
	private EmbeddedKafkaBroker broker;

	@MockitoSpyBean
	private NotificationService notificationService;

	@Test
	void contextLoads() {
	}

	@Test
	void recebePedidoDoKafkaENotifica() {
		// mesmo JSON que o order-service publica (sem header de tipo)
		String json = """
				{"orderId":"pedido-123","customerId":"CUSTOMER-1",
				 "items":[{"productId":"PROD-1","quantity":2,"price":49.90},
				          {"productId":"PROD-2","quantity":1,"price":120.00}],
				 "total":219.80,"createdAt":"2026-11-24T22:00:00Z"}
				""";

		var producerFactory = new DefaultKafkaProducerFactory<>(
				KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer());
		try {
			new KafkaTemplate<>(producerFactory).send(new ProducerRecord<>("order.created", "pedido-123", json));
		} finally {
			producerFactory.destroy();
		}

		ArgumentCaptor<OrderCreatedEvent> captor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
		verify(notificationService, timeout(15_000)).notificar(captor.capture());

		OrderCreatedEvent pedido = captor.getValue();
		assertThat(pedido.orderId()).isEqualTo("pedido-123");
		assertThat(pedido.customerId()).isEqualTo("CUSTOMER-1");
		assertThat(pedido.items()).hasSize(2);
		assertThat(pedido.items().getFirst().productId()).isEqualTo("PROD-1");
		assertThat(pedido.total()).isEqualByComparingTo(new BigDecimal("219.80"));
		assertThat(pedido.createdAt()).isEqualTo(Instant.parse("2026-11-24T22:00:00Z"));
	}
}
