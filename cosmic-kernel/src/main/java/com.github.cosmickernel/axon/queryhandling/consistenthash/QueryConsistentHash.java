package com.github.cosmickernel.axon.queryhandling.consistenthash;

import com.github.cosmickernel.axon.queryhandling.member.QueryMember;
import com.github.cosmickernel.axon.queryhandling.messagefilter.QueryMessageFilter;
import org.axonframework.common.Assert;
import org.axonframework.common.digest.Digester;
import org.axonframework.queryhandling.QueryMessage;

import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryConsistentHash {

    private final SortedMap<String, ConsistentHashMember> hashToMember;
    private final int modCount;
    private final UnaryOperator<String> hashFunction;
    private final Map<String, ConsistentHashMember> members;

    protected static String hash(String routingKey) {
        return Digester.md5Hex(routingKey);
    }

    public QueryConsistentHash() {
        this(QueryConsistentHash::hash);
    }

    public QueryConsistentHash(UnaryOperator<String> hashFunction) {
        hashToMember = Collections.emptySortedMap();
        members = Collections.emptyMap();
        modCount = 0;
        this.hashFunction = hashFunction;
    }

    private QueryConsistentHash(Map<String, ConsistentHashMember> members,
                                UnaryOperator<String> hashFunction, int modCount) {
        this.hashFunction = hashFunction;
        this.modCount = modCount;
        this.hashToMember = new TreeMap<>();
        this.members = members;
        members.values().forEach(m -> m.hashes().forEach(h -> hashToMember.put(h, m)));
    }

    public Collection<ConsistentHashMember> getEligibleMembers(String routingKey) {
        String hash = hash(routingKey);
        Collection<ConsistentHashMember> tail = hashToMember.tailMap(hash).values();
        Collection<ConsistentHashMember> head = hashToMember.headMap(hash).values();
        LinkedHashSet<ConsistentHashMember> combined = new LinkedHashSet<>(tail);
        combined.addAll(head);
        return combined;
    }

    public Set<QueryMember> getMembers(String routingKey, QueryMessage<?, ?> queryMessage) {
        String hash = hash(routingKey);
        Set<QueryMember> foundMembers = findSuitableMember(queryMessage, hashToMember.tailMap(hash).values());
        if (foundMembers.isEmpty()) {
            foundMembers = findSuitableMember(queryMessage, hashToMember.headMap(hash).values());
        }
        return foundMembers;
    }

    private Set<QueryMember> findSuitableMember(QueryMessage<?, ?> queryMessage,
                                                Collection<ConsistentHashMember> members) {
        return members.stream()
                .filter(member -> member.queryFilter.matches(queryMessage))
                .map(QueryMember.class::cast)
                .collect(Collectors.toSet());
    }

    public QueryConsistentHash with(QueryMember member, int loadFactor, QueryMessageFilter queryFilter) {
        Assert.notNull(member, () -> "Member may not be null");

        ConsistentHashMember newMember = new ConsistentHashMember(member, loadFactor, queryFilter);
        if (members.containsKey(member.name()) && newMember.equals(members.get(member.name()))) {
            return this;
        }

        Map<String, ConsistentHashMember> newMembers = new TreeMap<>(members);
        newMembers.put(member.name(), newMember);

        return new QueryConsistentHash(newMembers, hashFunction, modCount + 1);
    }

    public QueryConsistentHash without(QueryMember member) {
        Assert.notNull(member, () -> "Member may not be null");
        if (!members.containsKey(member.name())) {
            return this;
        }

        Map<String, ConsistentHashMember> newMembers = new TreeMap<>(members);
        newMembers.remove(member.name());
        return new QueryConsistentHash(newMembers, hashFunction, modCount + 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryConsistentHash that = (QueryConsistentHash) o;
        return modCount == that.modCount && Objects.equals(hashToMember, that.hashToMember) && Objects.equals(hashFunction, that.hashFunction) && Objects.equals(members, that.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashToMember, modCount, hashFunction, members);
    }

    @Override
    public String toString() {
        return "QueryConsistentHash{" +
                "hashToMember=" + hashToMember +
                ", modCount=" + modCount +
                ", hashFunction=" + hashFunction +
                ", members=" + members +
                '}';
    }

    public int version() {
        return modCount;
    }

    public static class ConsistentHashMember implements QueryMember {

        private final QueryMember member;
        private final int segmentCount;
        private final QueryMessageFilter queryFilter;

        private ConsistentHashMember(QueryMember member, int segmentCount,
                                     QueryMessageFilter queryFilter) {
            if (member instanceof ConsistentHashMember consistentHashMember) {
                this.member = (consistentHashMember).member;
            } else {
                this.member = member;
            }
            this.segmentCount = segmentCount;
            this.queryFilter = queryFilter;
        }

        @Override
        public String name() {
            return member.name();
        }

        @Override
        public boolean local() {
            return member.local();
        }

        @Override
        public void suspect() {
            member.suspect();
        }

        public int segmentCount() {
            return segmentCount;
        }

        public QueryMessageFilter getQueryFilter() {
            return queryFilter;
        }

        public Set<String> hashes() {
            return IntStream.range(0, segmentCount)
                    .mapToObj(i -> hash(name() + " #" + i))
                    .collect(Collectors.toSet());
        }

        @Override
        public <T> Optional<T> getConnectionEndpoint(Class<T> protocol) {
            return member.getConnectionEndpoint(protocol);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConsistentHashMember that = (ConsistentHashMember) o;
            return segmentCount == that.segmentCount && Objects.equals(member, that.member) && Objects.equals(queryFilter, that.queryFilter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(member, segmentCount, queryFilter);
        }

        @Override
        public String toString() {
            return "ConsistentHashMember{" +
                    "member=" + member +
                    ", segmentCount=" + segmentCount +
                    ", queryFilter=" + queryFilter +
                    '}';
        }
    }
}