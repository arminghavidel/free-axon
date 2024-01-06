package com.github.cosmickernel.axon.queryhandling.messagefilter;

import org.axonframework.queryhandling.QueryMessage;

public class NegateQueryMessageFilter implements QueryMessageFilter {

    private final QueryMessageFilter filter;

    public NegateQueryMessageFilter(QueryMessageFilter filter) {
        this.filter = filter;
    }

    @Override
    public boolean matches(QueryMessage<?, ?> queryMessage) {
        return !filter.matches(queryMessage);
    }
}
