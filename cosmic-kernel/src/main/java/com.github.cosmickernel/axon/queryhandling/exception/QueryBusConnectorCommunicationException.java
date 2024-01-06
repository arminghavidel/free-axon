package com.github.cosmickernel.axon.queryhandling.exception;

import org.axonframework.common.AxonTransientException;

public class QueryBusConnectorCommunicationException extends AxonTransientException {

    public QueryBusConnectorCommunicationException(String message) {
        super(message);
    }

    public QueryBusConnectorCommunicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
