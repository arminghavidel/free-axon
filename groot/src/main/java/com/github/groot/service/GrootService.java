package com.github.groot.service;

import org.axonframework.queryhandling.QueryGateway;
import org.springframework.stereotype.Service;
import query.GrootQuery;
import query.RocketResponse;

import java.util.concurrent.CompletableFuture;

@Service
public class GrootService {

    private final QueryGateway queryGateway;

    public GrootService(QueryGateway queryGateway) {
        this.queryGateway = queryGateway;
    }

    public CompletableFuture<RocketResponse> grootAsks() {
        var grootQuery = new GrootQuery();
        grootQuery.setQuery("I am Groot!");

        return queryGateway.query(grootQuery, RocketResponse.class);
    }
}
