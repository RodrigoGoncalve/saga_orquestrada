package br.com.microservices.orchestrated.paymentservice.core.service;

import br.com.microservices.orchestrated.paymentservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.paymentservice.core.dto.Event;
import br.com.microservices.orchestrated.paymentservice.core.dto.History;
import br.com.microservices.orchestrated.paymentservice.core.dto.OrderProduct;
import br.com.microservices.orchestrated.paymentservice.core.enuns.EpaymentStatus;
import br.com.microservices.orchestrated.paymentservice.core.model.Payment;
import br.com.microservices.orchestrated.paymentservice.core.producer.KafkaProducer;
import br.com.microservices.orchestrated.paymentservice.core.repository.PaymentRepository;
import br.com.microservices.orchestrated.paymentservice.core.utils.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static br.com.microservices.orchestrated.paymentservice.core.enuns.ESagaStatus.FAIL;
import static br.com.microservices.orchestrated.paymentservice.core.enuns.ESagaStatus.ROLLBACK_PENDING;
import static br.com.microservices.orchestrated.paymentservice.core.enuns.ESagaStatus.SUCCESS;

@Slf4j
@Service
@AllArgsConstructor
public class PaymentSevice {

    private static final String CURRENT_SOURCE = "PAYMENT_SERVICE";
    public static final Double REDUCE_SUM_VALUE = 0.0;
    public static final Double MIN_AMOUNT_VALUE = 0.1;

    private final JsonUtil jsonUtil;
    private final KafkaProducer kafkaProducer;
    private final PaymentRepository paymentRepository;

    public void realizePayment(Event event) {
        try {
            this.checkCurrentValidation(event);
            this.createPandingPayment(event);
            var payment = this.findByOrderIdAndTransactionId(event);
            this.validateAmounte(payment.getTotalAmount());
            this.changePaymenttoSuccess(payment);
            this.handleSuccess(event);
        } catch (Exception e) {
            log.error("Error trying to make payment: ", e);
            this.handleFailCurrentNotExacvuted(event, e.getMessage());
        }
        this.kafkaProducer.sendEvent(this.jsonUtil.toJson(event));
    }

    public void realizeRefund(Event event){
        event.setStatus(FAIL);
        event.setSource(CURRENT_SOURCE);
        try {
            this.changePaymentsStatusToRefund(event);
            addHistory(event, "Rollback executed for payment!");
        } catch (Exception e) {
            addHistory(event, "Rollback not executed for payment!".concat(e.getMessage()));
        }
        this.kafkaProducer.sendEvent(this.jsonUtil.toJson(event));
    }

    private void checkCurrentValidation(Event event) {
        if (this.paymentRepository.existsByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())) {
            throw new ValidationException("There's another tramsaction for this validation!");
        }
    }

    private void createPandingPayment(Event event) {
        var totalAmount = this.calculateTotalAmount(event);
        var totalItems = this.calculateTotalItems(event);

        var payment = Payment
                .builder()
                .orderId(event.getPayload().getId())
                .transactionId(event.getTransactionId())
                .totalAmount(totalAmount)
                .totalItems(totalItems)
                .build();
        this.save(payment);
        this.setEventAmountItens(event, payment);
    }

    private double calculateTotalAmount(Event event) {
        return event
                .getPayload()
                .getProducts()
                .stream()
                .map(product -> product.getQuantity() * product.getProduct().getUnitValue())
                .reduce(REDUCE_SUM_VALUE, Double::sum);
    }

    private int calculateTotalItems(Event event) {
        return event
                .getPayload()
                .getProducts()
                .stream()
                .map(OrderProduct::getQuantity)
                .reduce(REDUCE_SUM_VALUE.intValue(), Integer::sum);
    }

    private void setEventAmountItens(Event event, Payment payment) {
        event.getPayload().setTotalAmount(payment.getTotalAmount());
        event.getPayload().setTotalItens(payment.getTotalItems());
    }

    private void validateAmounte(double amount) {
        if (amount < MIN_AMOUNT_VALUE) {
            throw new ValidationException("The minimus amount available is ".concat(MIN_AMOUNT_VALUE.toString()));
        }
    }

    private void changePaymenttoSuccess(Payment payment) {
        payment.setStatus(EpaymentStatus.SUCCESS);
        this.save(payment);
    }

    private void handleSuccess(Event event) {
        event.setStatus(SUCCESS);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Payment realized successfully!");
    }

    private void addHistory(Event event, String message) {
        var hhistory = History.builder()
                .source(event.getSource())
                .status(event.getStatus())
                .message(message)
                .createdAt(LocalDateTime.now())
                .build();
        event.addToHistory(hhistory);
    }

    private void handleFailCurrentNotExacvuted(Event event, String message) {
        event.setStatus(ROLLBACK_PENDING);
        event.setSource(CURRENT_SOURCE);
        addHistory(event, "Fail to realize payment:  ".concat(message));
    }


    private void changePaymentsStatusToRefund(Event event) {
        var payment = this.findByOrderIdAndTransactionId(event);
        payment.setStatus(EpaymentStatus.REFUND);
        this.setEventAmountItens(event, payment);
        this.save(payment);
    }

    private Payment findByOrderIdAndTransactionId(Event event) {
        return this.paymentRepository
                .findByOrderIdAndTransactionId(event.getPayload().getId(), event.getTransactionId())
                .orElseThrow(() -> new ValidationException("Payment not found by OrderID and TransactionID"));
    }

    private void save(Payment payment) {
        this.paymentRepository.save(payment);
    }

}
