package com.chat.client;

import com.chat.grpc.*;
import com.chat.grpc.Number;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ChatClient {

    public static void main(String[] args) throws InterruptedException {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

//        ChatServiceGrpc.ChatServiceBlockingStub stub = ChatServiceGrpc.newBlockingStub(channel);

//        HelloRequest request = HelloRequest.newBuilder()
//                .setName("Jack")
//                .build();

        // unary RPC
//        HelloResponse response = stub.sayHello(request);
//        System.out.println(response.getMessage());

        // streaming RPC
//        Iterator<HelloResponse> responses = stub.streamGreetings(request);

//        while (responses.hasNext()) {
//            HelloResponse response = responses.next();
//            System.out.println(response.getMessage());
//        }

//        Client Streaming RPC

        ChatServiceGrpc.ChatServiceStub asyncStub = ChatServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1); // to wait until the response has arrived
        CountDownLatch joinLatch = new CountDownLatch(1); // to wait until the user has joined the chat
        AtomicBoolean joined = new AtomicBoolean(false); // to track if the user has joined the chat
        AtomicBoolean chatActive = new AtomicBoolean(true);

//        StreamObserver<SumResponse> responseObserver = new StreamObserver<>() {
/*
            @Override
            public void onNext(SumResponse response){
                System.out.println("Sum: " + response.getSum());
            }

            @Override
            public void onError(Throwable throwable){
                throwable.printStackTrace();
                latch.countDown();
            }

            @Override
            public void onCompleted(){
                System.out.println("Stream completed");
                latch.countDown();
            }
        };

        StreamObserver<Number> requestObserver = asyncStub.calculateSum(responseObserver);

        for (int i = 1; i <= 5; i++) {
            requestObserver.onNext(
                    Number.
                    newBuilder().
                    setValue(i).
                    build()
            );
            System.out.println("Sent: " + i);

            Thread.sleep(500); // Simulate delay
        }

 */


        AtomicReference<String> username = new AtomicReference<>();
        AtomicReference<String> activeRoom = new AtomicReference<>();
        Set<String> myRooms = ConcurrentHashMap.newKeySet();

        StreamObserver<ChatMessage> responseObserver = new StreamObserver<>(){
            @Override
            public void onNext(ChatMessage message){

                if(message.getType() == MessageType.PRIVATE){   // for private msg
                    if(message.getSender().equals(username.get())){
                        System.out.println("[PRIVATE To " + message.getRecipient() + " ]: " +
                                message.getContent());
                    }else{
                        System.out.println("[PRIVATE] " + message.getSender() + ": " +
                                message.getContent());
                    }
                    return;
                }

                System.out.println(message.getSender() + ": " + message.getContent());
            }

            @Override
            public void onError(Throwable throwable){
                chatActive.set(false);
                latch.countDown();
            }

            @Override
            public void onCompleted(){
                System.out.println("Chat ended");
                chatActive.set(false);
                latch.countDown();
            }
        };


        // start the chat stream
        StreamObserver<ChatMessage> requestObserver = asyncStub.chat(responseObserver);

        // send chat messages
/*        requestObserver.onNext(
                ChatMessage.newBuilder()
                        .setSender("Jack")
                        .setContent("Hello, chat!")
                        .build()
        );
        requestObserver.onNext(
                ChatMessage.newBuilder()
                        .setSender("Jack")
                        .setContent("How are you?")
                        .build()
        );
        requestObserver.onNext(
                ChatMessage.newBuilder()
                        .setSender("Jack")
                        .setContent("this is 3rd message!")
                        .build()
        );
 */

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter username:");
        username.set(scanner.nextLine());

        requestObserver.onNext(
                ChatMessage.newBuilder()
                        .setSender(username.get())
                        .setType(MessageType.JOIN)
                        .setContent(username + " has joined the chat")
                        .build()
        );

        while(chatActive.get()){
            String input = scanner.nextLine();
            if("exit".equalsIgnoreCase(input)){
                requestObserver.onCompleted();
                break;
            }

            // join room
            if(input.startsWith("/join ")){
                String roomName = input.substring(6).trim();
                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setSender(username.get())
                                .setRoom(roomName)
                                .setType(MessageType.ROOM_JOIN)
                                .build()
                );
                myRooms.add(roomName);
                continue;
            }

            // switch room
            if(input.startsWith("/switch ")){
                String roomName = input.substring(8).trim();

                if(!myRooms.contains(roomName)){
                    System.out.println("You are not a member of room: " + roomName);
                    continue;
                }

                activeRoom.set(roomName);
                System.out.println("Switched to room: " + roomName);
                continue;
            }

            // current room
            if(input.equals("/current")){
                System.out.println("Active room: "
                        + (activeRoom.get() != null ? activeRoom.get() : "None"));
                continue;
            }

            // leave room
            if(input.startsWith("/leave ")){
                String roomName = input.substring(7).trim();
                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setSender(username.get())
                                .setRoom(roomName)
                                .setType(MessageType.ROOM_LEAVE)
                                .build()
                );
                myRooms.remove(roomName);
                if(roomName.equals(activeRoom.get())){
                    activeRoom.set(null);
                }
                continue;
            }

            // private message
            if(input.startsWith("@")){
                int firstSpace = input.indexOf(' ');
                if(firstSpace == -1){
                    System.out.println("Invalid private message format. Use: @username message");
                    continue;
                }

                String recipient = input.substring(1, firstSpace);
                String content = input.substring(firstSpace + 1);

                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setSender(username.get())
                                .setRecipient(recipient)
                                .setType(MessageType.PRIVATE)
                                .setContent(content)
                                .build()
                );
                continue;
            }

            // broadcast message to everyone
            requestObserver.onNext(
                    ChatMessage.newBuilder()
                            .setSender(username.get())
                            .setType(MessageType.CHAT)
                            .setContent(input)
                            .build()
            );
        }

        latch.await(); //wait until the response has arrived then shutdown
        channel.shutdown();
    }
}
