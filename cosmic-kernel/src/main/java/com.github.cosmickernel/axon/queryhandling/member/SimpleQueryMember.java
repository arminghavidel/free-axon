package com.github.cosmickernel.axon.queryhandling.member;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

public class SimpleQueryMember<E> implements QueryMember {

    public static final Boolean LOCAL_MEMBER = true;

    public static final Boolean REMOTE_MEMBER = false;

    private final Consumer<SimpleQueryMember<E>> suspectHandler;
    private final String name;
    private final E endpoint;
    private final boolean local;

    public SimpleQueryMember(String name, E endpoint, boolean local, Consumer<SimpleQueryMember<E>> suspectHandler) {
        this.name = name;
        this.endpoint = endpoint;
        this.local = local;
        this.suspectHandler = suspectHandler;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public <T> Optional<T> getConnectionEndpoint(Class<T> protocol) {
        if (protocol.isInstance(endpoint)) {
            return Optional.of((T) endpoint);
        }
        return Optional.empty();
    }

    @Override
    public void suspect() {
        if (suspectHandler != null) {
            suspectHandler.accept(this);
        }
    }

    public E endpoint() {
        return endpoint;
    }

    @Override
    public boolean local() {
        return local;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleQueryMember<?> that = (SimpleQueryMember<?>) o;
        return local == that.local && Objects.equals(suspectHandler, that.suspectHandler) && Objects.equals(name, that.name) && Objects.equals(endpoint, that.endpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(suspectHandler, name, endpoint, local);
    }

    @Override
    public String toString() {
        return "SimpleQueryMember{" +
                "suspectHandler=" + suspectHandler +
                ", name='" + name + '\'' +
                ", endpoint=" + endpoint +
                ", local=" + local +
                '}';
    }
}
