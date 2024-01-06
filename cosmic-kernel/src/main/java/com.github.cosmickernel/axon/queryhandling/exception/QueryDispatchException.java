package com.github.cosmickernel.axon.queryhandling.exception;

import org.axonframework.common.AxonException;

public class QueryDispatchException extends AxonException {

    private static final long serialVersionUID = 4587368927394283730L;

    public QueryDispatchException(String message) {
        super(message);
    }

    public QueryDispatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
