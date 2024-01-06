package com.github.cosmickernel.axon.queryhandling;

import com.github.cosmickernel.axon.queryhandling.member.QueryMember;
import com.github.cosmickernel.axon.queryhandling.messagefilter.QueryMessageFilter;
import org.axonframework.queryhandling.QueryMessage;

import java.util.Set;

public interface QueryRouter {

    Set<QueryMember> findDestination(QueryMessage<?, ?> queryMessage);

    void updateMembership(int loadFactor, QueryMessageFilter queryFilter);
}
