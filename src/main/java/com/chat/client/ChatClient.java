package com.chat.client;

import com.chat.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ChatClient {

    private static final String HOST = "localhost";
    private static final int PORT = 9090;
    private static final String EXIT_CMD = "exit";
    private static final String HELP_CMD = "/help";
    private static final String JOIN_CMD = "/join ";
    private static final String SWITCH_CMD = "/switch ";
    private static final String CURRENT_CMD = "/current";
    private static final String LEAVE_CMD = "/leave ";
    private static final String PRIVATE_PREFIX = "@";
    private static final String ROOM_PREFIX = "#";

    public static void main(String[] args) throws InterruptedException {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(HOST, PORT)
                .usePlaintext()
                .build();

        ChatServiceGrpc.ChatServiceStub asyncStub = ChatServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1); // to wait until the response has arrived
        AtomicBoolean chatActive = new AtomicBoolean(true);

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

                // room chat
                if(message.getType() == MessageType.ROOM_CHAT){
                    System.out.println("[" + message.getRoom() + "] " + message.getSender() + ": " +
                            message.getContent());
                    return;
                }

                // global chat
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

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter username:");
        username.set(scanner.nextLine());
        System.out.println("Type /help to see the available commands.");

        requestObserver.onNext(
                ChatMessage.newBuilder()
                        .setSender(username.get())
                        .setType(MessageType.JOIN)
                        .setContent(username + " has joined the chat")
                        .build()
        );

        while(chatActive.get()){
            String input = scanner.nextLine();

            if(EXIT_CMD.equalsIgnoreCase(input)){
                requestObserver.onCompleted();
                break;
            }

            // show all available commands
            if(HELP_CMD.equalsIgnoreCase(input)){
                printHelp();
                continue;
            }

            // join room
            if(input.startsWith(JOIN_CMD)){
                String roomName = input.substring(JOIN_CMD.length()).trim();
                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setRoom(roomName)
                                .setType(MessageType.ROOM_JOIN)
                                .build()
                );
                myRooms.add(roomName);
                continue;
            }

            // switch room
            if(input.startsWith(SWITCH_CMD)){
                String roomName = input.substring(SWITCH_CMD.length()).trim();

                if(!myRooms.contains(roomName)){
                    System.out.println("You are not a member of room: " + roomName);
                    continue;
                }

                activeRoom.set(roomName);
                System.out.println("Switched to room: " + roomName);
                continue;
            }

            // current room
            if(input.equalsIgnoreCase(CURRENT_CMD)){
                System.out.println("Active room: "
                        + (activeRoom.get() != null ? activeRoom.get() : "None"));
                continue;
            }

            // leave room
            if(input.startsWith(LEAVE_CMD)){
                String roomName = input.substring(LEAVE_CMD.length()).trim();
                requestObserver.onNext(
                        ChatMessage.newBuilder()
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
            if(input.startsWith(PRIVATE_PREFIX)){
                int firstSpace = input.indexOf(' ');
                if(firstSpace == -1){
                    System.out.println("Invalid private message format. Use: @username message");
                    continue;
                }

                String recipient = input.substring(PRIVATE_PREFIX.length(), firstSpace);
                String content = input.substring(firstSpace + 1);

                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setRecipient(recipient)
                                .setType(MessageType.PRIVATE)
                                .setContent(content)
                                .build()
                );
                continue;
            }

            // room message
            if(input.startsWith(ROOM_PREFIX)){
                int firstSpace = input.indexOf(' ');
                if(firstSpace == -1){
                    System.out.println("Invalid room message format. Use: #roomname message");
                    continue;
                }

                String roomName = input.substring(ROOM_PREFIX.length(), firstSpace);
                String content = input.substring(firstSpace + 1);

                if(!myRooms.contains(roomName)){
                    System.out.println("You are not a member of room: " + roomName);
                    continue;
                }

                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setRoom(roomName)
                                .setType(MessageType.ROOM_CHAT)
                                .setContent(content)
                                .build()
                );
                continue;
            }

            // attatch the sender by server not the client (for both room n normal chat)
            if(activeRoom.get() != null) {
                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setRoom(activeRoom.get())
                                .setType(MessageType.ROOM_CHAT)
                                .setContent(input)
                                .build()
                );
            }
            else{
                requestObserver.onNext(
                        ChatMessage.newBuilder()
                                .setType(MessageType.CHAT)
                                .setContent(input)
                                .build()
                );
            }
        }

        latch.await(); //wait until the response has arrived then shutdown
        channel.shutdown();
    }

    private static void printHelp(){
        System.out.println("""
                Available commands:
                  /join <room>      - join a room
                  /switch <room>    - switch to a room you joined
                  /current          - show your active room
                  /leave <room>     - leave a room
                  @<user> <msg>     - private message a user
                  #<room> <msg>     - message a specific room
                  /help             - show this help
                  exit              - leave the chat
                """);
    }
}
