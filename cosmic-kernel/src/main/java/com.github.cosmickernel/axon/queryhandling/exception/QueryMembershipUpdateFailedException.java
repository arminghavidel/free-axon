package com.github.cosmickernel.axon.queryhandling.exception;

import org.axonframework.common.AxonException;

public class QueryMembershipUpdateFailedException extends AxonException {

    private static final long serialVersionUID = -433655641071800433L;

    public QueryMembershipUpdateFailedException(String message) {
        super(message);
    }

    public QueryMembershipUpdateFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}