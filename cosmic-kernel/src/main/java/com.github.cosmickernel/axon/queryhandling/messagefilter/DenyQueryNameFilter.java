package com.github.cosmickernel.axon.queryhandling.messagefilter;

import org.axonframework.queryhandling.QueryMessage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DenyQueryNameFilter implements QueryMessageFilter {

    private final Set<String> queryNames;

    public DenyQueryNameFilter(Set<String> queryNames) {
        this.queryNames = new HashSet<>(queryNames);
    }

    public DenyQueryNameFilter(String queryName) {
        this.queryNames = Collections.singleton(queryName);
    }

    @Override
    public boolean matches(QueryMessage<?, ?> queryMessage) {
        return !queryNames.contains(queryMessage.getQueryName());
    }

    @Override
    public QueryMessageFilter and(QueryMessageFilter other) {
        if (other instanceof DenyQueryNameFilter denyQueryNameFilter) {
            return new DenyQueryNameFilter(
                    Stream.concat(queryNames.stream(), (denyQueryNameFilter).queryNames.stream())
                            .collect(Collectors.toSet()));
        } else {
            return new AndQueryMessageFilter(this, other);
        }
    }

    @Override
    public QueryMessageFilter or(QueryMessageFilter other) {
        if (other instanceof DenyQueryNameFilter denyQueryNameFilter) {
            return new DenyQueryNameFilter(
                    queryNames.stream().filter((denyQueryNameFilter).queryNames::contains)
                            .collect(Collectors.toSet()));
        } else {
            return new OrQueryMessageFilter(this, other);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DenyQueryNameFilter that = (DenyQueryNameFilter) o;
        return Objects.equals(queryNames, that.queryNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryNames);
    }

    @Override
    public String toString() {
        return "DenyQueryNameFilter{" +
                "queryNames=" + queryNames +
                '}';
    }
}
