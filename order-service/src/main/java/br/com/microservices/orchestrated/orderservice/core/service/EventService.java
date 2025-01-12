package br.com.microservices.orchestrated.orderservice.core.service;

import br.com.microservices.orchestrated.orderservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.orderservice.core.document.Event;
import br.com.microservices.orchestrated.orderservice.core.dto.EventFilters;
import br.com.microservices.orchestrated.orderservice.core.repository.EventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

import static org.springframework.util.ObjectUtils.isEmpty;

@Slf4j
@Service
@AllArgsConstructor
public class EventService {

    private final EventRepository eventRepository;

    public void notifyEnting(Event event){
        event.setOrderId(event.getOrderId());
        event.setCreatedAt(LocalDateTime.now());
        log.info("Order {} with saga notified! TransactionaId: {}", event.getOrderId(), event.getTransactionId());
    }

    public List<Event> findAll(){
        return eventRepository.findAllByOrderByCreatedAtDesc();
    }

    public Event findByFilters(EventFilters filters){
        validateEmptyFilters(filters);
        if(!isEmpty(filters.getOrderId())){
            return findByOrderid(filters.getOrderId());
        }else {
            return findByTransactionalId(filters.getTransactionId());
        }
    }

    private Event findByOrderid(String orderId){
        return eventRepository.findTop1ByOrderIdOrderByCreatedAtDesc(orderId)
                .orElseThrow(() -> new ValidationException("Event not found by orderId. "));

    }

    private Event findByTransactionalId(String transactionalId){
        return eventRepository.findTop1ByTransactionIdOrderByCreatedAtDesc(transactionalId)
                .orElseThrow(() -> new ValidationException("Event not found by transactionalId. "));

    }


    private void validateEmptyFilters(EventFilters filters){
        if (isEmpty(filters.getOrderId()) && isEmpty(filters.getTransactionId())){
            throw new ValidationException("Orderid or TransactionId must be informed.");
        }
    }

    public Event save(Event event){
        return eventRepository.save(event);
    }

}
