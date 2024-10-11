package br.com.microservices.orchestrated.orderservice.core.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SagaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic.start-saga}")
    private String startSagaTopic;

    public void sendEvent(String payLoad){
        try {
            log.info("Sending event to topic {} with data {}", startSagaTopic, payLoad);
            kafkaTemplate.send(startSagaTopic, payLoad);
        } catch (Exception e) {
            log.error("Erro trying to send data to topic {} with data {}", startSagaTopic, payLoad, e);
        }
    }

}
