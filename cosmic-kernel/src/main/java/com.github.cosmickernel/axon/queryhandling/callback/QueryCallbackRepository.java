package com.github.cosmickernel.axon.queryhandling.callback;

import  com.github.cosmickernel.axon.queryhandling.exception.QueryBusConnectorCommunicationException;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.axonframework.queryhandling.GenericQueryResponseMessage.asResponseMessage;


public class QueryCallbackRepository<A> {
    private final Map<String, QueryCallbackWrapper> callbacks = new ConcurrentHashMap<>();

    public void cancelCallbacks(A channelId) {
        Iterator<QueryCallbackWrapper> callbackWrapperIterator = this.callbacks.values().iterator();
        while (callbackWrapperIterator.hasNext()) {
            QueryCallbackWrapper wrapper = callbackWrapperIterator.next();
            if (wrapper.getChannelIdentifier().equals(channelId)) {
                wrapper.reportResult(asResponseMessage(new QueryBusConnectorCommunicationException(
                        String.format("Connection error while waiting for a response on query %s",
                                wrapper.getMessage().getQueryName()))));
                callbackWrapperIterator.remove();
            }
        }
    }

    public <E, C, R> QueryCallbackWrapper<E, C, R> fetchAndRemove(String callbackId) {
        return callbacks.remove(callbackId);
    }

    public <E, C, R> void store(String callbackId, QueryCallbackWrapper<E, C, R> queryCallbackWrapper) {
        QueryCallbackWrapper<E,C,R> previous;
        if ((previous = callbacks.put(callbackId, queryCallbackWrapper)) != null) {
            previous.reportResult(asResponseMessage(new QueryBusConnectorCommunicationException(
                    "Query-callback cancelled, a new query with the same ID is entered into the query bus")));
        }
    }

    protected Map<String, QueryCallbackWrapper> callbacks() {
        return callbacks;
    }
}
