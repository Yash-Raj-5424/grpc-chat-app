package com.chat.service;

import com.chat.grpc.*;
import com.chat.grpc.Number;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;

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

    @Override
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver){

        return new StreamObserver<>(){

            private String username;

            @Override
            public void onNext(ChatMessage message){

                if(message.getType() == MessageType.JOIN){
                    username = message.getSender();
                    clients.put(username, responseObserver);

                    System.out.println(username + " joined. Total clients: " + clients.size());
                    return;
                }
                if(message.getType() == MessageType.CHAT){
                    System.out.println(message.getSender() + ": " + message.getContent());

                    for(StreamObserver<ChatMessage> observer: clients.values()){
                        observer.onNext(message);
                    }
                }
            }

            @Override
            public void onError(Throwable throwable){
                if(username != null){
                    clients.remove(username);
                    System.out.println(username + " disconnected: " + throwable.getMessage());
                }
            }

            @Override
            public void onCompleted(){
                if(username != null){
                    clients.remove(username);
                    System.out.println(username + " left");
                }
                responseObserver.onCompleted();
            }
        };
    }

    private final ConcurrentHashMap<String, StreamObserver<ChatMessage>> clients =
            new ConcurrentHashMap<>();


}
