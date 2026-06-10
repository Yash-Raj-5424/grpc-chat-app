package com.chat.service;

import com.chat.grpc.ChatServiceGrpc;
import com.chat.grpc.HelloRequest;
import com.chat.grpc.HelloResponse;
import com.chat.grpc.SumResponse;
import io.grpc.stub.StreamObserver;
import com.chat.grpc.Number;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {

    @Override
    public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver){

        String name = request.getName();
        HelloResponse response = HelloResponse.newBuilder()
                .setMessage("Hello, " + name + "!")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void streamGreetings(HelloRequest request, StreamObserver<HelloResponse> responseObserver){

        String name = request.getName();
        for (int i = 1; i <= 5; i++) {
            HelloResponse response = HelloResponse.newBuilder()
                    .setMessage("Greeting " + i + " for " + name)
                    .build();
            responseObserver.onNext(response);
            try {
                Thread.sleep(1000); // Simulate delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<Number> calculateSum(StreamObserver<SumResponse> responseObserver){

        return new StreamObserver<Number>() {
            int sum = 0;

            @Override
            public void onNext(Number number) {
                sum += number.getValue();
                System.out.println("Received: " + number.getValue());
            }

            @Override
            public void onError(Throwable throwable) {
                System.err.println("Client Error: " + throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                SumResponse response = SumResponse.newBuilder()
                        .setSum(sum)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

}
