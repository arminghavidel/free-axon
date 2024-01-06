package com.github.rocket.config.service;

import org.axonframework.queryhandling.QueryHandler;
import org.springframework.stereotype.Component;
import query.GrootQuery;
import query.RocketResponse;

@Component
public class RocketQueryHandler {


    @QueryHandler
    public RocketResponse rocketResponse(GrootQuery grootQuery) {
        var rocketResponse = new RocketResponse();
        rocketResponse.setResponse("Yeah! Of course buddy!");
        return rocketResponse;
    }
}
