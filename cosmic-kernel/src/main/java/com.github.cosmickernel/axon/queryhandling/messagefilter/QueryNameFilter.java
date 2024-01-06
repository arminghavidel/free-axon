package com.github.cosmickernel.axon.queryhandling.messagefilter;

import org.axonframework.queryhandling.QueryMessage;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryNameFilter implements QueryMessageFilter {

    private final Set<String> queryNames;

    public QueryNameFilter(Set<String> queryNames) {
        this.queryNames = queryNames;
    }

    public QueryNameFilter(String queryName) {
        this(Collections.singleton(queryName));
    }

    @Override
    public boolean matches(QueryMessage<?, ?> queryMessage) {
        return queryNames.contains(queryMessage.getQueryName());
    }

    @Override
    public QueryMessageFilter negate() {
        return new DenyQueryNameFilter(queryNames);
    }

    @Override
    public QueryMessageFilter and(QueryMessageFilter other) {
        if (other instanceof QueryNameFilter queryNameFilter) {
            return new QueryNameFilter(queryNames.stream()
                    .filter((queryNameFilter).queryNames::contains)
                    .collect(Collectors.toSet()));
        } else {
            return t -> matches(t) && other.matches(t);
        }
    }

    @Override
    public QueryMessageFilter or(QueryMessageFilter other) {
        if (other instanceof QueryNameFilter queryNameFilter) {
            return new QueryNameFilter(
                    Stream.concat(
                                    queryNames.stream(),
                                    (queryNameFilter).queryNames.stream())
                            .collect(Collectors.toSet()));
        } else {
            return new OrQueryMessageFilter(this, other);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryNameFilter that = (QueryNameFilter) o;
        return Objects.equals(queryNames, that.queryNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryNames);
    }

    @Override
    public String toString() {
        return "QueryNameFilter{" +
                "queryNames=" + queryNames +
                '}';
    }
}
