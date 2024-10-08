package br.com.microservices.orchestrated.orchestratorservice.core.dto;

import br.com.microservices.orchestrated.orchestratorservice.core.enuns.EEventSource;
import br.com.microservices.orchestrated.orchestratorservice.core.enuns.ESagaStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Event {

    private String id;
    private String transactionId;
    private String orderId;
    private Order payload;
    private EEventSource source;
    private ESagaStatus status;
    private List<History> eventHistory;
    private LocalDateTime createdAt;
}
