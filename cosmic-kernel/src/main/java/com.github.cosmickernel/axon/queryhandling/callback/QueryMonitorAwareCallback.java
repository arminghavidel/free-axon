package com.github.cosmickernel.axon.queryhandling.callback;

import org.axonframework.monitoring.MessageMonitor;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;

public class QueryMonitorAwareCallback<C, R> implements QueryCallback<C, R> {

    private final QueryCallback<C, R> delegate;
    private final MessageMonitor.MonitorCallback messageMonitorCallback;

    public QueryMonitorAwareCallback(QueryCallback<C, R> delegate, MessageMonitor.MonitorCallback messageMonitorCallback) {
        this.delegate = delegate;
        this.messageMonitorCallback = messageMonitorCallback;
    }

    @Override
    public void onResult(QueryMessage<?, ? extends C> queryMessage,
                         QueryResponseMessage<? extends R> queryResponseMessage) {
        if (queryResponseMessage.isExceptional()) {
            messageMonitorCallback.reportFailure(queryResponseMessage.exceptionResult());
        } else {
            messageMonitorCallback.reportSuccess();
        }
        if (delegate != null) {
            delegate.onResult(queryMessage, queryResponseMessage);
        }
    }

}
