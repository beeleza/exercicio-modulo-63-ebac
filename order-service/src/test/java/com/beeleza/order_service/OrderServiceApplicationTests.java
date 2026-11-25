package com.beeleza.order_service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = "order.created")
class OrderServiceApplicationTests {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private EmbeddedKafkaBroker broker;

	@Test
	void contextLoads() {
	}

	@Test
	void criarPedidoPublicaEventoNoKafka() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-group", true);
		try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
				props, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
			broker.consumeFromAnEmbeddedTopic(consumer, "order.created");

			mockMvc.perform(post("/orders"))
					.andExpect(status().isAccepted())
					.andExpect(jsonPath("$.orderId").exists())
					.andExpect(jsonPath("$.total").value(219.80));

			ConsumerRecord<String, String> record =
					KafkaTestUtils.getSingleRecord(consumer, "order.created", Duration.ofSeconds(10));

			assertThat(record.key()).isNotBlank();
			assertThat(record.value())
					.contains("\"orderId\":\"" + record.key() + "\"")
					.contains("\"customerId\":\"CUSTOMER-1\"")
					.contains("PROD-1");
			assertThat(record.headers().lastHeader("__TypeId__")).isNull();
		}
	}
}
