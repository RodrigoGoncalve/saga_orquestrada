package br.com.microservices.orchestrated.productvalidationservice.core.service;

import br.com.microservices.orchestrated.productvalidationservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.productvalidationservice.core.dto.Event;
import br.com.microservices.orchestrated.productvalidationservice.core.dto.History;
import br.com.microservices.orchestrated.productvalidationservice.core.dto.OrderProduct;
import br.com.microservices.orchestrated.productvalidationservice.core.model.Validation;
import br.com.microservices.orchestrated.productvalidationservice.core.producer.KafkaProducer;
import br.com.microservices.orchestrated.productvalidationservice.core.repository.ProductRepository;
import br.com.microservices.orchestrated.productvalidationservice.core.repository.ValidationRepository;
import br.com.microservices.orchestrated.productvalidationservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.productvalidationservice.core.enuns.ESagaStatus.FAIL;
import static br.com.microservices.orchestrated.productvalidationservice.core.enuns.ESagaStatus.ROLLBACK_PENDING;
import static br.com.microservices.orchestrated.productvalidationservice.core.enuns.ESagaStatus.SUCCESS;
import static org.springframework.util.ObjectUtils.isEmpty;

@Slf4j
@Service
@AllArgsConstructor
public class ProductValidationService {

    private static final String CURRENT_SOURCE = "PRODUCT-VALIDATION-SERVICE";

    private final JsonUtil jsonUtil;
    private final KafkaProducer kafkaProducer;
    private final ProductRepository productRepository;
    private final ValidationRepository validationRepository;


    public void validateExistingProducts(Event event) {
        try {
            this.checkCurrentValidation(event);
            this.createValiation(event, true);
            this.handleSuccess(event);

        } catch (Exception ex) {
            log.error("Error trying to validate products: ", ex);
            this.handleFailCurrentNotExacvuted(event, ex.getMessage());
        }
        this.kafkaProducer.sendEvent(jsonUtil.toJason(event));
    }

    private void validateProductsInformed(Event event) {
        if (isEmpty(event.getPayload()) || isEmpty(event.getPayload().getProducts())) {
            throw new ValidationException("Product is empty!");
        }
        if (isEmpty(event.getPayload().getId()) || isEmpty(event.getPayload().getTransactionId())) {
            throw new ValidationException("Order ID or Transaction ID is empty!");
        }
    }

    private void checkCurrentValidation(Event event) {
        this.validateProductsInformed(event);
        if (this.validationRepository.existsByOrderIdAndTransactionId(event.getOrderId(), event.getId())) {
            throw new ValidationException("There's another tramsaction for this validation!");
        }

        event.getPayload().getProducts().forEach(product -> {
            this.validationProductInformed(product);
            this.validationExistingProduct(product.getProduct().getCode());
        });
    }

    private void validationProductInformed(OrderProduct product) {
        if (isEmpty(product.getProduct()) || isEmpty(product.getProduct().getCode())) {
            throw new ValidationException("Product must be informed!");
        }
    }

    private void validationExistingProduct(String code) {
        if (this.productRepository.existsByCode(code)) {
            throw new ValidationException(String.format("Product does not exists in database!"));
        }
    }

    private void createValiation(Event event, boolean success) {
        var validation = Validation.builder().orderId(event.getPayload().getId()).transactionId(event.getPayload().getTransactionId()).success(success).build();
        this.validationRepository.save(validation);
    }

    private void handleSuccess(Event event) {
        event.setStatus(SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Products are validated successfully!");

    }

    private void addHistory(Event event, String message) {
        var hhistory = History.builder().source(event.getSource()).status(event.getStatus()).message(message).createdAt(LocalDateTime.now()).build();
        event.addToHistory(hhistory);
    }

    private void handleFailCurrentNotExacvuted(Event event, String message) {
        event.setStatus(ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Fail to validate products:  ".concat(message));
    }

    public void rollbackEvent(Event event) {
        this.changeValidationToFail(event);
        event.setStatus(FAIL);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Rollback executed on product validation!");
        this.kafkaProducer.sendEvent(jsonUtil.toJason(event));
    }

    private void changeValidationToFail(Event event) {
        this.validationRepository
                .findByOrderIdAndTransactionId(
                        event.getPayload().getId(),
                        event.getPayload().getTransactionId()).
                        ifPresentOrElse(validation -> {
            validation.setSuccess(false);
            this.validationRepository.save(validation);
        }, () -> this.createValiation(event, false));
    }

}
