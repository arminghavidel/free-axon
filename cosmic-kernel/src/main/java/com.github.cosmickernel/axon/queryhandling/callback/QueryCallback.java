package com.github.cosmickernel.axon.queryhandling.callback;

import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;

@FunctionalInterface
public interface QueryCallback<C, R> {
    void onResult(QueryMessage<?, ? extends C> queryMessage, QueryResponseMessage<? extends R> queryResponseMessage);
}
