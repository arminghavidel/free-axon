package com.github.cosmickernel.axon.queryhandling.mono;

import reactor.core.publisher.MonoSink;

public class MonoSinkWrapper<T> {

    private final MonoSink<T> monoSink;

    MonoSinkWrapper(MonoSink<T> monoSink) {
        this.monoSink = monoSink;
    }

    public void success(T value) {
        monoSink.success(value);
    }

    public void error(Throwable t) {
        monoSink.error(t);
    }
}
