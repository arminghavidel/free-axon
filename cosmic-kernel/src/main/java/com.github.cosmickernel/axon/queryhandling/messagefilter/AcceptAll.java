package com.github.cosmickernel.axon.queryhandling.messagefilter;

import org.axonframework.queryhandling.QueryMessage;

public enum AcceptAll implements QueryMessageFilter {

    INSTANCE;

    @Override
    public boolean matches(QueryMessage<?, ?> queryMessage) {
        return true;
    }

    @Override
    public QueryMessageFilter and(QueryMessageFilter other) {
        return other;
    }

    @Override
    public QueryMessageFilter negate() {
        return DenyAll.INSTANCE;
    }


    @Override
    public QueryMessageFilter or(QueryMessageFilter other) {
        return this;
    }

    @Override
    public String toString() {
        return "AcceptAll{}";
    }
}
