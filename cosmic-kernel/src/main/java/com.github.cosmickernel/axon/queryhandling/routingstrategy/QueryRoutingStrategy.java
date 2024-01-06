package com.github.cosmickernel.axon.queryhandling.routingstrategy;

import org.axonframework.queryhandling.QueryMessage;

public interface QueryRoutingStrategy {

    String getRoutingKey(QueryMessage<?, ?> query);
}
