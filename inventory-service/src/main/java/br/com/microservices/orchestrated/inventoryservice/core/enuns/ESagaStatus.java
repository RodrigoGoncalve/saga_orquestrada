package br.com.microservices.orchestrated.inventoryservice.core.enuns;

public enum ESagaStatus {

    SUCCESS,
    ROLLBACK_PENDING,
    FAIL;
}
