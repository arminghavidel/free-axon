package com.github.cosmickernel.axon.queryhandling.routingstrategy;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.ReflectionUtils;
import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.queryhandling.QueryMessage;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.axonframework.common.ReflectionUtils.*;

public class AnnotationQueryRoutingStrategy extends AbstractQueryRoutingStrategy {

    private static final RoutingKeyResolver NO_RESOLVE = new RoutingKeyResolver((Method) null);
    private static final String NULL_DEFAULT = null;

    private final Class<? extends Annotation> annotationType;
    private final Map<Class<?>, RoutingKeyResolver> resolverMap = new ConcurrentHashMap<>();

    public AnnotationQueryRoutingStrategy() {
        this(QueryRoutingKey.class);
    }

    public AnnotationQueryRoutingStrategy(Class<? extends Annotation> annotationType) {
        this(annotationType, UnresolvedQueryRoutingKeyPolicy.ERROR);
    }

    public AnnotationQueryRoutingStrategy(UnresolvedQueryRoutingKeyPolicy unresolvedRoutingKeyPolicy) {
        this(QueryRoutingKey.class, unresolvedRoutingKeyPolicy);
    }

    public AnnotationQueryRoutingStrategy(Class<? extends Annotation> annotationType,
                                          UnresolvedQueryRoutingKeyPolicy unresolvedRoutingKeyPolicy) {
        super(unresolvedRoutingKeyPolicy);
        this.annotationType = annotationType;
    }

    @Override
    protected String doResolveRoutingKey(QueryMessage<?, ?> query) {
        String routingKey;
        try {
            routingKey = findIdentifier(query);
        } catch (InvocationTargetException e) {
            throw new AxonConfigurationException(
                    "An exception occurred while extracting routing information form a query", e
            );
        } catch (IllegalAccessException e) {
            throw new AxonConfigurationException(
                    "The current security context does not allow extraction of routing information from the given query.",
                    e
            );
        }
        return routingKey;
    }

    private String findIdentifier(QueryMessage<?, ?> query) throws InvocationTargetException, IllegalAccessException {
        return resolverMap.computeIfAbsent(query.getPayloadType(), this::createResolver)
                .identify(query.getPayload());
    }

    private RoutingKeyResolver createResolver(Class<?> type) {
        for (Method m : methodsOf(type)) {
            if (AnnotationUtils.findAnnotationAttributes(m, annotationType).isPresent()) {
                ensureAccessible(m);
                return new RoutingKeyResolver(m);
            }
        }
        for (Field f : fieldsOf(type)) {
            if (AnnotationUtils.findAnnotationAttributes(f, annotationType).isPresent()) {
                return new RoutingKeyResolver(f);
            }
        }
        return NO_RESOLVE;
    }

    private static final class RoutingKeyResolver {

        private final Method method;
        private final Field field;

        public RoutingKeyResolver(Method method) {
            this.method = method;
            this.field = null;
        }

        public RoutingKeyResolver(Field field) {
            this.method = null;
            this.field = field;
        }

        public String identify(Object query) throws InvocationTargetException, IllegalAccessException {
            if (method != null) {
                return Objects.toString(method.invoke(query), NULL_DEFAULT);
            } else if (field != null) {
                return Objects.toString(ReflectionUtils.getFieldValue(field, query), NULL_DEFAULT);
            }
            return null;
        }
    }
}
