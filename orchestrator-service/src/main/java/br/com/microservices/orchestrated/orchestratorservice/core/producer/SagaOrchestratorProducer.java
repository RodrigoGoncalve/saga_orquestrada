package br.com.microservices.orchestrated.orchestratorservice.core.producer;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class SagaOrchestratorProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendEvent(String payLoad, String toppic){
        try {
            log.info("Sending event to topic {} with data {}", toppic, payLoad);
            kafkaTemplate.send(toppic, payLoad);
        } catch (Exception e) {
            log.error("Erro trying to send data to topic {} with data {}", toppic, payLoad, e);
        }
    }

}
