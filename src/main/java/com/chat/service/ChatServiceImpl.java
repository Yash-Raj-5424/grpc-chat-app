package com.chat.service;

import com.chat.grpc.ChatServiceGrpc;
import com.chat.grpc.HelloRequest;
import com.chat.grpc.HelloResponse;
import io.grpc.stub.StreamObserver;

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

}
