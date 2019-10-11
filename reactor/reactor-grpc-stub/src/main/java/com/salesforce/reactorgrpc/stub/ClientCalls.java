/*
 *  Copyright (c) 2019, Salesforce.com, Inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc.stub;

import java.util.function.BiConsumer;
import java.util.function.Function;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;

/**
 * Utility functions for processing different client call idioms. We have one-to-one correspondence
 * between utilities in this class and the potential signatures in a generated stub client class so
 * that the runtime can vary behavior without requiring regeneration of the stub.
 */
public final class ClientCalls {
    private ClientCalls() {

    }

    /**
     * Implements a unary → unary call using {@link Mono} → {@link Mono}.
     */
    public static <TRequest, TResponse> Mono<TResponse> oneToOne(
            Mono<TRequest> monoSource,
            BiConsumer<TRequest, StreamObserver<TResponse>> delegate) {
        try {
            return monoSource.flatMap(request -> Mono.create(callback -> delegate.accept(request, new StreamObserver<TResponse>() {
                @Override
                public void onNext(TResponse tResponse) {
                    callback.success(tResponse);
                }

                @Override
                public void onError(Throwable throwable) {
                    callback.error(throwable);
                }

                @Override
                public void onCompleted() {
                    // do nothing
                }
            })));
        } catch (Throwable throwable) {
            return Mono.error(throwable);
        }
    }

    /**
     * Implements a unary → stream call as {@link Mono} → {@link Flux}, where the server responds with a
     * stream of messages.
     */
    public static <TRequest, TResponse> Flux<TResponse> oneToMany(
            Mono<TRequest> monoSource,
            BiConsumer<TRequest, StreamObserver<TResponse>> delegate) {
        try {
            return monoSource
                    .flatMapMany(request -> {
                        ReactorClientStreamObserverAndPublisher<TResponse> consumerStreamObserver =
                            new ReactorClientStreamObserverAndPublisher<>(null);

                        delegate.accept(request, consumerStreamObserver);

                        return consumerStreamObserver;
                    });
        } catch (Throwable throwable) {
            return Flux.error(throwable);
        }
    }

    /**
     * Implements a stream → unary call as {@link Flux} → {@link Mono}, where the client transits a stream of
     * messages.
     */
    @SuppressWarnings("unchecked")
    public static <TRequest, TResponse> Mono<TResponse> manyToOne(
            Flux<TRequest> fluxSource,
            Function<StreamObserver<TResponse>, StreamObserver<TRequest>> delegate) {
        try {
            ReactorSubscriberAndClientProducer<TRequest> subscriberAndGRPCProducer =
                    fluxSource.subscribeWith(new ReactorSubscriberAndClientProducer<>());
            ReactorClientStreamObserverAndPublisher<TResponse> observerAndPublisher =
                    new ReactorClientStreamObserverAndPublisher<>(
                        s -> subscriberAndGRPCProducer.subscribe((CallStreamObserver<TRequest>) s),
                        subscriberAndGRPCProducer::cancel
                    );
            delegate.apply(observerAndPublisher);

            return Flux.from(observerAndPublisher)
                       .singleOrEmpty();
        } catch (Throwable throwable) {
            return Mono.error(throwable);
        }
    }

    /**
     * Implements a bidirectional stream → stream call as {@link Flux} → {@link Flux}, where both the client
     * and the server independently stream to each other.
     */
    @SuppressWarnings("unchecked")
    public static <TRequest, TResponse> Flux<TResponse> manyToMany(
            Flux<TRequest> fluxSource,
            Function<StreamObserver<TResponse>, StreamObserver<TRequest>> delegate) {
        try {
            ReactorSubscriberAndClientProducer<TRequest> subscriberAndGRPCProducer =
                fluxSource.subscribeWith(new ReactorSubscriberAndClientProducer<>());
            ReactorClientStreamObserverAndPublisher<TResponse> observerAndPublisher =
                new ReactorClientStreamObserverAndPublisher<>(
                    s -> subscriberAndGRPCProducer.subscribe((CallStreamObserver<TRequest>) s),
                    subscriberAndGRPCProducer::cancel
                );
            delegate.apply(observerAndPublisher);

            return Flux.from(observerAndPublisher);
        } catch (Throwable throwable) {
            return Flux.error(throwable);
        }
    }
}
