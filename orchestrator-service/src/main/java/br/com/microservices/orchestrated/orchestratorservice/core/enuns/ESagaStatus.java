package br.com.microservices.orchestrated.orchestratorservice.core.enuns;

public enum ESagaStatus {

    SUCCESS,
    ROLLBACK_PENDING,
    FAIL;
}
