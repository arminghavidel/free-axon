package com.github.cosmickernel.axon.queryhandling;

import com.github.cosmickernel.axon.queryhandling.callback.QueryCallback;
import com.github.cosmickernel.axon.queryhandling.member.QueryMember;
import org.axonframework.common.Registration;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptorSupport;
import org.axonframework.queryhandling.QueryMessage;

import java.lang.reflect.Type;

public interface QueryBusConnector extends MessageHandlerInterceptorSupport<QueryMessage<?, ?>> {

    <Q, R> void send(QueryMember destination, QueryMessage<?, ?> queryMessage, QueryCallback<? super Q, R> callback) throws Exception;

    <R> Registration subscribe(String var1, Type type, MessageHandler<? super QueryMessage<?, R>> handler);
}
