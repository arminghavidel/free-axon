package com.github.rocket.config;

import com.github.cosmickernel.axon.queryhandling.DistributedQueryBus;
import com.github.cosmickernel.axon.queryhandling.JGroupsQueryConnector;
import com.github.cosmickernel.axon.queryhandling.routingstrategy.AnnotationQueryRoutingStrategy;
import com.github.cosmickernel.axon.queryhandling.routingstrategy.UnresolvedQueryRoutingKeyPolicy;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.distributed.AnnotationRoutingStrategy;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.commandhandling.distributed.UnresolvedRoutingKeyPolicy;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.extensions.jgroups.commandhandling.JGroupsConnector;
import org.axonframework.queryhandling.DefaultQueryGateway;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryGateway;
import org.axonframework.queryhandling.SimpleQueryBus;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.jgroups.JChannel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;

public class AxonConfig {

    @Bean
    @Scope("prototype")
    public JChannel jChannel() throws Exception {
        return new JChannel();
    }

    @Bean
    public JGroupsConnector jGroupsConnector(JChannel jChannel) throws Exception {
        CommandBus localSegment = SimpleCommandBus.builder().build();
        XStreamSerializer.Builder builder = XStreamSerializer.builder();
        JGroupsConnector.Builder jGroupsConnectorBuilder = JGroupsConnector.builder();
        jGroupsConnectorBuilder.localSegment(localSegment)
                .channel(jChannel)
                .clusterName("jgroupsClusterName")
                .serializer(builder.build())
                .routingStrategy(new AnnotationRoutingStrategy(UnresolvedRoutingKeyPolicy.RANDOM_KEY));

        JGroupsConnector connector = jGroupsConnectorBuilder.build();
        connector.connect();
        return connector;
    }

    @Bean
    @Primary
    public CommandBus distributedCommandBus(JGroupsConnector jGroupsConnector) {
        DistributedCommandBus.Builder builder = DistributedCommandBus.builder();
        builder.connector(jGroupsConnector)
                .commandRouter(jGroupsConnector);
        return builder.build();
    }

    @Bean
    public CommandGateway commandGateway(CommandBus commandBus) {
        DefaultCommandGateway.Builder builder = DefaultCommandGateway.builder();
        builder.commandBus(commandBus);
        return builder.build();
    }

    @Bean
    public JGroupsQueryConnector jGroupsQueryConnector(JChannel jChannel) throws Exception {
        QueryBus localSegment = SimpleQueryBus.builder().build();
        XStreamSerializer.Builder builder = XStreamSerializer.builder();
        JGroupsQueryConnector.Builder jGroupsQueryConnectorBuilder = JGroupsQueryConnector.builder();
        jGroupsQueryConnectorBuilder.localSegment(localSegment)
                .channel(jChannel)
                .clusterName("jgroupsClusterName")
                .serializer(builder.build())
                .routingStrategy(new AnnotationQueryRoutingStrategy(UnresolvedQueryRoutingKeyPolicy.RANDOM_KEY));

        JGroupsQueryConnector connector = jGroupsQueryConnectorBuilder.build();
        connector.connect();
        return connector;
    }

    @Bean
    @Primary
    public QueryBus distributedQueryBus(JGroupsQueryConnector jGroupsQueryConnector) {
        DistributedQueryBus.Builder builder = DistributedQueryBus.builder();
        builder.connector(jGroupsQueryConnector)
                .queryRouter(jGroupsQueryConnector);
        return builder.build();
    }

    @Bean
    public QueryGateway queryGateway(QueryBus queryBus) {
        DefaultQueryGateway.Builder builder = DefaultQueryGateway.builder();
        builder.queryBus(queryBus);
        return builder.build();
    }
}
