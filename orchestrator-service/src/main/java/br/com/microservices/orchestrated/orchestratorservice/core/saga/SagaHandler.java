package br.com.microservices.orchestrated.orchestratorservice.core.saga;

import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.EEventSource.INVENTORY_SERVICE;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.EEventSource.ORCHESTRATOR;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.EEventSource.PAYMENT_SERVICE;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.EEventSource.PRODUCT_VALIDATION_SERVICE;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ESagaStatus.FAIL;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ESagaStatus.ROLLBACK_PENDING;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ESagaStatus.SUCCESS;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.FINISH_FAIL;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.FINISH_SUCCESS;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.INVENTORY_FAIL;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.INVENTORY_SUCCESS;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.PAYMENT_FAIL;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.PAYMENT_SUCCESS;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.PRODUCT_VALIDATION_FAIL;
import static br.com.microservices.orchestrated.orchestratorservice.core.enuns.ETopics.PRODUCT_VALIDATION_SUCCESS;

public final class SagaHandler {

    private SagaHandler ( ) {

    }

    public static final Object [][] SAGA_HANDLER  =  {

            {ORCHESTRATOR, SUCCESS, PRODUCT_VALIDATION_SUCCESS},
            {ORCHESTRATOR, FAIL, FINISH_FAIL},

            {PRODUCT_VALIDATION_SERVICE, ROLLBACK_PENDING, PRODUCT_VALIDATION_FAIL},
            {PRODUCT_VALIDATION_SERVICE, FAIL, FINISH_FAIL},
            {PRODUCT_VALIDATION_SERVICE, SUCCESS, PAYMENT_SUCCESS},

            {PAYMENT_SERVICE, ROLLBACK_PENDING, PAYMENT_FAIL},
            {PAYMENT_SERVICE, FAIL, PRODUCT_VALIDATION_FAIL},
            {PAYMENT_SERVICE, SUCCESS, INVENTORY_SUCCESS},

            {INVENTORY_SERVICE, ROLLBACK_PENDING, INVENTORY_FAIL},
            {INVENTORY_SERVICE, FAIL, PAYMENT_FAIL},
            {INVENTORY_SERVICE, SUCCESS, FINISH_SUCCESS},
    };

        public static final int EVENT_SOURCE_INDEX = 0;
        public static final int SAGA_STATUS_INDEX = 1;
        public static final int TOPIC_INDEX = 2;
}
