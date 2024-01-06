package com.github.cosmickernel.axon.queryhandling.exception;

import org.axonframework.common.AxonException;

public class QueryServiceRegistryException extends AxonException {

    public QueryServiceRegistryException(String message) {
        super(message);
    }

    public QueryServiceRegistryException(String message, Throwable cause) {
        super(message, cause);
    }
}
