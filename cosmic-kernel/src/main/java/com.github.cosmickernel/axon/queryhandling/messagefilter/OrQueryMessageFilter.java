package com.github.cosmickernel.axon.queryhandling.messagefilter;

import org.axonframework.queryhandling.QueryMessage;

public class OrQueryMessageFilter implements QueryMessageFilter {
    private final QueryMessageFilter first;
    private final QueryMessageFilter second;

    public OrQueryMessageFilter(QueryMessageFilter first, QueryMessageFilter second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean matches(QueryMessage<?, ?> queryMessage) {
        return first.matches(queryMessage) || second.matches(queryMessage);
    }
}
