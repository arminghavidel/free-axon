package com.github.cosmickernel.axon.queryhandling.messagefilter;

import org.axonframework.queryhandling.QueryMessage;

public enum DenyAll implements QueryMessageFilter {

    INSTANCE;

    @Override
    public boolean matches(QueryMessage<?, ?> queryMessage) {
        return false;
    }

    @Override
    public QueryMessageFilter and(QueryMessageFilter other) {
        return this;
    }


    @Override
    public QueryMessageFilter negate() {
        return AcceptAll.INSTANCE;
    }

    @Override
    public QueryMessageFilter or(QueryMessageFilter other) {
        return other;
    }

    @Override
    public String toString() {
        return "DenyAll{}";
    }
}
