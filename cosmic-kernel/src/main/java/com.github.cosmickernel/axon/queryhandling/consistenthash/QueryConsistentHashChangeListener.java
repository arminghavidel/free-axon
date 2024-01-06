package com.github.cosmickernel.axon.queryhandling.consistenthash;

@FunctionalInterface
public interface QueryConsistentHashChangeListener {

    void onConsistentHashChanged(QueryConsistentHash newConsistentHash);

    static QueryConsistentHashChangeListener noOp() {
        return newQueryConsistentHash -> {
        };
    }
}
