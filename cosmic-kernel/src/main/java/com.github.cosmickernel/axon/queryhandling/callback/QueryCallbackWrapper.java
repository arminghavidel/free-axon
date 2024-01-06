package com.github.cosmickernel.axon.queryhandling.callback;

import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;

public class QueryCallbackWrapper<A, C, R> implements QueryCallback<C, R> {

    private final A sessionId;
    private final QueryMessage<?, C> message;
    private final QueryCallback<? super C, ? super R> wrapped;

    public QueryCallbackWrapper(A sessionId, QueryMessage<?, C> message, QueryCallback<? super C, ? super R> wrapped) {
        this.sessionId = sessionId;
        this.message = message;
        this.wrapped = wrapped;
    }

    public QueryMessage<?, C> getMessage() {
        return message;
    }

    public A getChannelIdentifier() {
        return sessionId;
    }

    public void reportResult(QueryResponseMessage<R> result) {
        onResult(getMessage(), result);
    }

    @Override
    public void onResult(QueryMessage<?, ? extends C> message, QueryResponseMessage<? extends R> queryResponseMessage) {
        wrapped.onResult(message, queryResponseMessage);
    }
}
