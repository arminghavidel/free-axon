package com.github.cosmickernel.axon.queryhandling.callback;

import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.axonframework.queryhandling.GenericQueryResponseMessage.asResponseMessage;

public class QueryFutureCallback<C, R> extends CompletableFuture<QueryResponseMessage<? extends R>>
        implements QueryCallback<C, R> {

    @Override
    public void onResult(QueryMessage<?, ? extends C> queryMessage, QueryResponseMessage<? extends R> queryResponseMessage) {
        super.complete(queryResponseMessage);
    }

    public QueryResponseMessage<? extends R> getResult() {
        try {
            return get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return asResponseMessage(e.getCause());
        } catch (ExecutionException e) {
            return asResponseMessage(e.getCause());
        } catch (Exception e) {
            return asResponseMessage(e);
        }
    }

    public QueryResponseMessage<? extends R> getResult(long timeout, TimeUnit unit) {
        try {
            return get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return asResponseMessage(e.getCause());
        } catch (ExecutionException e) {
            return asResponseMessage(e.getCause());
        } catch (Exception e) {
            return asResponseMessage(e);
        }
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) {
        try {
            get(timeout, unit);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            return true;
        } catch (TimeoutException e) {
            return false;
        }
    }
}
