package com.github.cosmickernel.axon.queryhandling.member;

import java.util.Optional;

public interface QueryMember {

    String name();

    <T> Optional<T> getConnectionEndpoint(Class<T> protocol);

    boolean local();

    void suspect();

}
