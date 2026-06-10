package com.chat.client;

import com.chat.grpc.ChatServiceGrpc;
import com.chat.grpc.HelloRequest;
import com.chat.grpc.HelloResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChatClient {

    public static void main(String[] args) {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

        ChatServiceGrpc.ChatServiceBlockingStub stub = ChatServiceGrpc.newBlockingStub(channel);

        HelloRequest request = HelloRequest.newBuilder()
                .setName("Jack")
                .build();

        HelloResponse response = stub.sayHello(request);

        System.out.println(response.getMessage());

        channel.shutdown();
    }
}
