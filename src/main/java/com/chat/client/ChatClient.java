package com.chat.client;

import com.chat.grpc.ChatServiceGrpc;
import com.chat.grpc.HelloRequest;
import com.chat.grpc.HelloResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;

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

        // unary RPC
//        HelloResponse response = stub.sayHello(request);
//        System.out.println(response.getMessage());

        // streaming RPC
        Iterator<HelloResponse> responses = stub.streamGreetings(request);
        
        while (responses.hasNext()) {
            HelloResponse response = responses.next();
            System.out.println(response.getMessage());
        }

        channel.shutdown();
    }
}
