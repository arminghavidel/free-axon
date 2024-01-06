package com.github.cosmickernel.axon.queryhandling.exception;

import org.axonframework.common.AxonException;

public class QueryConnectionFailedException extends AxonException {

    private static final long serialVersionUID = -5193897502103028801L;

    public QueryConnectionFailedException(String message) {
        super(message);
    }

    public QueryConnectionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
