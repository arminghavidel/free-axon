package com.github.cosmickernel.axon.queryhandling.routingstrategy;

import com.github.cosmickernel.axon.queryhandling.exception.QueryDispatchException;
import org.axonframework.common.Assert;
import org.axonframework.queryhandling.QueryMessage;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public abstract class AbstractQueryRoutingStrategy implements QueryRoutingStrategy {

    private static final String STATIC_ROUTING_KEY = "unresolved";

    private final UnresolvedQueryRoutingKeyPolicy unresolvedQueryRoutingKeyPolicy;
    private final AtomicLong counter = new AtomicLong(0);

    AbstractQueryRoutingStrategy(UnresolvedQueryRoutingKeyPolicy unresolvedQueryRoutingKeyPolicy) {
        Assert.notNull(unresolvedQueryRoutingKeyPolicy, () -> "UnresolvedQueryRoutingKeyPolicy may not be null");
        this.unresolvedQueryRoutingKeyPolicy = unresolvedQueryRoutingKeyPolicy;
    }

    @Override
    public String getRoutingKey(QueryMessage<?, ?> query) {
        String routingKey = doResolveRoutingKey(query);
        if (routingKey == null) {
            switch (unresolvedQueryRoutingKeyPolicy) {
                case ERROR:
                    throw new QueryDispatchException(format("The query [%s] does not contain a routing key.",
                            query.getQueryName()));
                case RANDOM_KEY:
                    return Long.toHexString(counter.getAndIncrement());
                case STATIC_KEY:
                    return STATIC_ROUTING_KEY;
                default:
                    throw new IllegalStateException("The configured UnresolvedRoutingPolicy of "
                            + unresolvedQueryRoutingKeyPolicy.name() + " is not supported.");
            }
        }
        return routingKey;
    }

    protected abstract String doResolveRoutingKey(QueryMessage<?, ?> query);

}
