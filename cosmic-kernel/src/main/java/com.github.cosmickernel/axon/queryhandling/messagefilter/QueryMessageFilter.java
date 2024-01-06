package com.github.cosmickernel.axon.queryhandling.messagefilter;

import org.axonframework.queryhandling.QueryMessage;

import java.io.Serializable;

public interface QueryMessageFilter extends Serializable {

    boolean matches(QueryMessage<?, ?> queryMessage);

    default QueryMessageFilter and(QueryMessageFilter other) {
        return new AndQueryMessageFilter(this, other);
    }

    default QueryMessageFilter negate() {
        return new NegateQueryMessageFilter(this);
    }

    default QueryMessageFilter or(QueryMessageFilter other) {
        return new OrQueryMessageFilter(this, other);
    }
}
