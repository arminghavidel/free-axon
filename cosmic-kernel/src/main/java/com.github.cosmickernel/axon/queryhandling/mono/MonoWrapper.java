package com.github.cosmickernel.axon.queryhandling.mono;


import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public class MonoWrapper<T> {

    private final Mono<T> mono;

    MonoWrapper(Mono<T> mono) {
        this.mono = mono;
    }

    public Mono<T> getMono() {
        return mono;
    }

    public static <T> MonoWrapper<T> create(Consumer<MonoSinkWrapper<T>> callback) {
        return new MonoWrapper<>(Mono.create(monoSink -> callback.accept(new MonoSinkWrapper<>(monoSink))));
    }
}
