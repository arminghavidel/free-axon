package com.github.cosmickernel.axon.queryhandling;

import com.github.cosmickernel.axon.queryhandling.callback.QueryFutureCallback;
import com.github.cosmickernel.axon.queryhandling.callback.QueryMonitorAwareCallback;
import com.github.cosmickernel.axon.queryhandling.exception.QueryDispatchException;
import com.github.cosmickernel.axon.queryhandling.member.QueryMember;
import com.github.cosmickernel.axon.queryhandling.messagefilter.DenyAll;
import com.github.cosmickernel.axon.queryhandling.messagefilter.DenyQueryNameFilter;
import com.github.cosmickernel.axon.queryhandling.messagefilter.QueryMessageFilter;
import com.github.cosmickernel.axon.queryhandling.messagefilter.QueryNameFilter;
import org.axonframework.commandhandling.CommandExecutionException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.monitoring.MessageMonitor;
import org.axonframework.monitoring.NoOpMessageMonitor;
import org.axonframework.queryhandling.*;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.queryhandling.GenericQueryResponseMessage.asResponseMessage;

public class DistributedQueryBus implements QueryBus {

    private static final String DISPATCH_ERROR_MESSAGE = "An error occurred while trying to dispatch a query ";
    public static final int INITIAL_LOAD_FACTOR = 100;

    private final QueryRouter queryRouter;
    private final QueryBusConnector connector;
    private final QueryUpdateEmitter queryUpdateEmitter;
    private final MessageMonitor<? super QueryMessage<?, ?>> messageMonitor;
    private final List<MessageDispatchInterceptor<? super QueryMessage<?, ?>>> dispatchInterceptors = new CopyOnWriteArrayList<>();
    private final AtomicReference<QueryMessageFilter> queryFilter = new AtomicReference<>(DenyAll.INSTANCE);
    private final List<MessageHandlerInterceptor<? super QueryMessage<?, ?>>> handlerInterceptors = new CopyOnWriteArrayList<>();

    protected DistributedQueryBus(Builder builder) {
        builder.validate();

        this.queryRouter = builder.queryRouter;
        this.connector = builder.connector;

        this.messageMonitor = builder.messageMonitor;
        this.queryUpdateEmitter = builder.queryUpdateEmitter;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <R> Registration subscribe(String queryName,
                                      Type responseType,
                                      MessageHandler<? super QueryMessage<?, R>> handler) {
        Registration reg = connector.subscribe(queryName, responseType, handler);
        updateFilter(queryFilter.get().or(new QueryNameFilter(queryName)));
        return () -> {
            updateFilter(queryFilter.get().and(new DenyQueryNameFilter(queryName)));
            return reg.cancel();
        };
    }

    private void updateFilter(QueryMessageFilter newFilter) {
        if (!queryFilter.getAndSet(newFilter).equals(newFilter)) {
            queryRouter.updateMembership(INITIAL_LOAD_FACTOR, newFilter);
        }
    }

    @Override
    public <Q, R> CompletableFuture<QueryResponseMessage<R>> query(QueryMessage<Q, R> query) {

        QueryMessage<Q, R> interceptedQuery = intercept(query);
        MessageMonitor.MonitorCallback monitorCallback = messageMonitor.onMessageIngested(query);
        Set<QueryMember> members = queryRouter.findDestination(interceptedQuery);

        for (QueryMember member : members) {

            QueryFutureCallback<Object, R> futureCallback = new QueryFutureCallback<>();
            try {
                connector.send(
                        member,
                        interceptedQuery,
                        new QueryMonitorAwareCallback<>(futureCallback, monitorCallback));
                QueryResponseMessage<? extends R> queryResponseMessage = futureCallback.getResult();
                if (queryResponseMessage.isExceptional()) {
                    throw asRuntime(queryResponseMessage.exceptionResult());
                }
                return (CompletableFuture<QueryResponseMessage<R>>) queryResponseMessage.getPayload();

            } catch (Exception e) {
                monitorCallback.reportFailure(e);
                member.suspect();
                futureCallback.onResult(interceptedQuery, asResponseMessage(
                        new QueryDispatchException(DISPATCH_ERROR_MESSAGE + ": " + e.getMessage(), e)
                ));
            }
        }

        throw new NoHandlerForQueryException(
                format("No handler found for [%s] with response type [%s]",
                        interceptedQuery.getQueryName(),
                        interceptedQuery.getResponseType())
        );
    }

    private RuntimeException asRuntime(Throwable e) {
        if (e instanceof Error error) {
            throw error;
        } else if (e instanceof RuntimeException exception) {
            return exception;
        } else {
            return new CommandExecutionException("An exception occurred while executing a query", e);
        }
    }

    @Override
    public <Q, R> Stream<QueryResponseMessage<R>> scatterGather(QueryMessage<Q, R> query, long timeout, TimeUnit unit) {
        return null;
    }

    @Override
    public <Q, I, U> SubscriptionQueryResult<QueryResponseMessage<I>, SubscriptionQueryUpdateMessage<U>> subscriptionQuery(
            SubscriptionQueryMessage<Q, I, U> query,
            SubscriptionQueryBackpressure backpressure,
            int updateBufferSize) {
        return null;
    }

    @Override
    public QueryUpdateEmitter queryUpdateEmitter() {
        return queryUpdateEmitter;
    }

    private <R> CompletableFuture<QueryResponseMessage<R>> buildCompletableFuture(ResponseType<R> responseType,
                                                                                  Object queryResponse) {
        return CompletableFuture.completedFuture(GenericQueryResponseMessage.asNullableResponseMessage(
                responseType.responseMessagePayloadType(),
                responseType.convert(queryResponse)));
    }

    private <Q, R, T extends QueryMessage<Q, R>> T intercept(T query) {
        T intercepted = query;
        for (MessageDispatchInterceptor<? super QueryMessage<?, ?>> interceptor : dispatchInterceptors) {
            intercepted = (T) interceptor.handle(intercepted);
        }
        return intercepted;
    }

    @Override
    public Registration registerHandlerInterceptor(MessageHandlerInterceptor<? super QueryMessage<?, ?>> interceptor) {
        handlerInterceptors.add(interceptor);
        return () -> handlerInterceptors.remove(interceptor);
    }

    @Override
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super QueryMessage<?, ?>> interceptor) {
        dispatchInterceptors.add(interceptor);
        return () -> dispatchInterceptors.remove(interceptor);
    }

    public static class Builder {
        private QueryRouter queryRouter;
        private QueryBusConnector connector;
        private QueryUpdateEmitter queryUpdateEmitter;
        private MessageMonitor<? super QueryMessage<?, ?>> messageMonitor = NoOpMessageMonitor.INSTANCE;

        public Builder queryRouter(QueryRouter queryRouter) {
            assertNonNull(queryRouter, "QueryRouter may not be null");
            this.queryRouter = queryRouter;
            return this;
        }

        public Builder queryUpdateEmitter(QueryUpdateEmitter queryUpdateEmitter) {
            assertNonNull(queryUpdateEmitter, "QueryUpdateEmitter may not be null");
            this.queryUpdateEmitter = queryUpdateEmitter;
            return this;
        }

        public Builder messageMonitor(MessageMonitor<? super QueryMessage<?, ?>> monitor) {
            assertNonNull(monitor, "MessageMonitor may not be null");
            this.messageMonitor = monitor;
            return this;
        }

        public Builder connector(QueryBusConnector connector) {
            assertNonNull(connector, "CommandBusConnector may not be null");
            this.connector = connector;
            return this;
        }

        public DistributedQueryBus build() {
            return new DistributedQueryBus(this);
        }

        protected void validate() throws AxonConfigurationException {
        }
    }
}
