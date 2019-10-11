/*
 *  Copyright (c) 2019, Salesforce.com, Inc.
 *  All rights reserved.
 *  Licensed under the BSD 3-Clause license.
 *  For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.reactorgrpc;

import com.salesforce.grpc.testing.contrib.NettyGrpcServerRule;
import org.junit.Rule;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorContextPropagationTest {

    @Rule
    public NettyGrpcServerRule serverRule = new NettyGrpcServerRule();

    private static class SimpleGreeter extends ReactorGreeterGrpc.GreeterImplBase {
        @Override
        public Mono<HelloResponse> sayHello(Mono<HelloRequest> request) {
            return request.map(HelloRequest::getName)
                    .map(name -> HelloResponse.newBuilder().setMessage("Hello " + name).build());
        }

        @Override
        public Mono<HelloResponse> sayHelloReqStream(Flux<HelloRequest> request) {
            return request.map(HelloRequest::getName)
                    .collect(Collectors.joining("and"))
                    .map(names -> HelloResponse.newBuilder().setMessage("Hello " + names).build());
        }

        @Override
        public Flux<HelloResponse> sayHelloRespStream(Mono<HelloRequest> request) {
            return request.repeat(2)
                    .map(HelloRequest::getName)
                    .zipWith(Flux.just("Hello ", "Hi ", "Greetings "), String::join)
                    .map(greeting -> HelloResponse.newBuilder().setMessage(greeting).build());
        }

        @Override
        public Flux<HelloResponse> sayHelloBothStream(Flux<HelloRequest> request) {
            return request.map(HelloRequest::getName)
                    .map(name -> HelloResponse.newBuilder().setMessage("Hello " + name).build());
        }
    }

    @Test
    public void oneToOne() {
        serverRule.getServiceRegistry().addService(new SimpleGreeter());

        ReactorGreeterGrpc.ReactorGreeterStub stub = ReactorGreeterGrpc.newReactorStub(serverRule.getChannel());
        Mono<HelloRequest> req = Mono.just(HelloRequest.newBuilder().setName("reactor").build());

        Mono<HelloResponse> resp = req
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .compose(stub::sayHello)
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .subscriberContext(Context.of("name", "context"));

        StepVerifier.create(resp)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    public void oneToMany() {
        serverRule.getServiceRegistry().addService(new SimpleGreeter());

        ReactorGreeterGrpc.ReactorGreeterStub stub = ReactorGreeterGrpc.newReactorStub(serverRule.getChannel());
        Mono<HelloRequest> req = Mono.just(HelloRequest.newBuilder().setName("reactor").build());

        Flux<HelloResponse> resp = req
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .as(stub::sayHelloRespStream)
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .subscriberContext(Context.of("name", "context"));

        StepVerifier.create(resp)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    public void manyToOne() {
        serverRule.getServiceRegistry().addService(new SimpleGreeter());

        ReactorGreeterGrpc.ReactorGreeterStub stub = ReactorGreeterGrpc.newReactorStub(serverRule.getChannel());
        Flux<HelloRequest> req = Mono.just(HelloRequest.newBuilder().setName("reactor").build()).repeat(2);

        Mono<HelloResponse> resp = req
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .as(stub::sayHelloReqStream)
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .subscriberContext(Context.of("name", "context"));

        StepVerifier.create(resp)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    public void manyToMany() {
        serverRule.getServiceRegistry().addService(new SimpleGreeter());

        ReactorGreeterGrpc.ReactorGreeterStub stub = ReactorGreeterGrpc.newReactorStub(serverRule.getChannel());
        Flux<HelloRequest> req = Mono.just(HelloRequest.newBuilder().setName("reactor").build()).repeat(2);

        Flux<HelloResponse> resp = req
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .compose(stub::sayHelloBothStream)
                .doOnEach(signal -> assertThat(signal.getContext().getOrEmpty("name")).isNotEmpty())
                .subscriberContext(Context.of("name", "context"));

        StepVerifier.create(resp)
                .expectNextCount(3)
                .verifyComplete();
    }
}
