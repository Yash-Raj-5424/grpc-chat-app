package com.chat.service;

import com.chat.grpc.*;
import com.chat.grpc.Number;
import io.grpc.stub.StreamObserver;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {

    private final ConcurrentHashMap<String, Set<String>> rooms = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> userRooms = new ConcurrentHashMap<>();

//    @Override
//    public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver){
//
//        String name = request.getName();
//        HelloResponse response = HelloResponse.newBuilder()
//                .setMessage("Hello, " + name + "!")
//                .build();
//
//        responseObserver.onNext(response);
//        responseObserver.onCompleted();
//    }
//
//    @Override
//    public void streamGreetings(HelloRequest request, StreamObserver<HelloResponse> responseObserver){
//
//        String name = request.getName();
//        for (int i = 1; i <= 5; i++) {
//            HelloResponse response = HelloResponse.newBuilder()
//                    .setMessage("Greeting " + i + " for " + name)
//                    .build();
//            responseObserver.onNext(response);
//            try {
//                Thread.sleep(1000); // Simulate delay
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//        }
//
//        responseObserver.onCompleted();
//    }
//
//    @Override
//    public StreamObserver<Number> calculateSum(StreamObserver<SumResponse> responseObserver){
//
//        return new StreamObserver<Number>() {
//            int sum = 0;
//
//            @Override
//            public void onNext(Number number) {
//                sum += number.getValue();
//                System.out.println("Received: " + number.getValue());
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                System.err.println("Client Error: " + throwable.getMessage());
//            }
//
//            @Override
//            public void onCompleted() {
//                SumResponse response = SumResponse.newBuilder()
//                        .setSum(sum)
//                        .build();
//                responseObserver.onNext(response);
//                responseObserver.onCompleted();
//            }
//        };
//    }

    @Override
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver){

        return new StreamObserver<>(){

            private String username;

            @Override
            public void onNext(ChatMessage message){

                if(message.getType() == MessageType.JOIN){
                    username = message.getSender();

                    StreamObserver<ChatMessage> existing = clients.putIfAbsent(username, responseObserver);
                    if(existing != null){   // prevent duplicate usernames
                        responseObserver.onNext(
                                ChatMessage.newBuilder()
                                        .setSender("System")
                                        .setType(MessageType.SYSTEM)
                                        .setContent("Username: '" + username + "' is already taken. Reconnect and try a different one.")
                                        .build()
                        );
                        responseObserver.onCompleted(); // close the stream
                        return;
                    }

                    responseObserver.onNext(    // client sees join success
                            ChatMessage.newBuilder()
                                    .setSender("System")
                                    .setType(MessageType.SYSTEM)
                                    .setContent("JOIN_SUCCESS")
                                    .build()
                    );

                    System.out.println(username + " joined");

                    ChatMessage joinNotification = ChatMessage.newBuilder()
                            .setType(MessageType.JOIN)
                            .setSender(username)
                            .setContent("------ " + username + " joined the chat -------")
                            .build();

                    for(var entry: clients.entrySet()){ // broadcast join notification to others
                        if(!entry.getKey().equals(username)){
                            entry.getValue().onNext(joinNotification);
                        }
                    }
                    return;
                }

                // room join
                if(message.getType() == MessageType.ROOM_JOIN){
                    String room = message.getRoom();

                    // room name validation
                    if(room.contains(" ")){
                        System.out.println("Invalid room name: '" + room + "'. Room names cannot contain spaces");
                        responseObserver.onNext(
                                ChatMessage.newBuilder()
                                        .setSender("System")
                                        .setType(MessageType.SYSTEM)
                                        .setContent("Invalid room name: '" + room + "'. Room names cannot contain spaces")
                                        .build()
                        );
                        return;
                    }

                    rooms.computeIfAbsent(room, k -> ConcurrentHashMap.newKeySet()).add(username);
                    userRooms.computeIfAbsent(username, k -> ConcurrentHashMap.newKeySet()).add(room);

                    System.out.println(username + " joined room: " + room);

                    responseObserver.onNext(
                            ChatMessage.newBuilder()
                                    .setSender("System")
                                    .setType(MessageType.SYSTEM)
                                    .setContent("Joined room: '" + room + "'")
                                    .build()
                    );
                    return;
                }

                // room leave
                if(message.getType() == MessageType.ROOM_LEAVE){

                    String room = message.getRoom();
                    Set<String> roomMembers = rooms.get(room);
                    Set<String> joinedRooms = userRooms.get(username);

                    if(joinedRooms == null || !joinedRooms.contains(room)){
                        responseObserver.onNext(
                                ChatMessage.newBuilder()
                                        .setSender("System")
                                        .setType(MessageType.SYSTEM)
                                        .setContent("You are not in room: '" + room + "'")
                                        .build()
                        );
                        return;
                    }

                    if(roomMembers != null){
                        roomMembers.remove(username);
                        if(roomMembers.isEmpty()){
                            rooms.remove(room); // remove the empty room
                        }
                    }

                    if(joinedRooms != null){
                        joinedRooms.remove(room);
                        if(joinedRooms.isEmpty()){
                            userRooms.remove(username);
                        }
                    }


                    System.out.println(username + " left room: '" + room + "'");

                    responseObserver.onNext(
                            ChatMessage.newBuilder()
                                    .setSender("System")
                                    .setType(MessageType.SYSTEM)
                                    .setContent("Left room: '" + room + "'")
                                    .build()
                    );
                    return;
                }

                // room chat
                if(message.getType() == MessageType.ROOM_CHAT){
                    String room = message.getRoom();
                    Set<String> roomMembers = rooms.get(room);

                    if(roomMembers == null){
                        responseObserver.onNext(
                                ChatMessage.newBuilder()
                                        .setSender("System")
                                        .setType(MessageType.SYSTEM)
                                        .setContent("Room '" + room + "' does not exist")
                                        .build()
                        );
                        return;
                    }

                    System.out.println("[ROOM: " + room + "] "
                            + message.getSender() + ": " + message.getContent());

                    for(String member: roomMembers){
                        StreamObserver<ChatMessage> observer = clients.get(member);
                        if(observer != null){
                            observer.onNext(message);
                        }
                    }
                    return;
                }

                // global chat
                if(message.getType() == MessageType.CHAT){  // broadcast messaging
                    System.out.println(message.getSender() + ": " + message.getContent());

                    for(StreamObserver<ChatMessage> observer: clients.values()){
                        observer.onNext(message);
                    }
                }

                // private messaging
                if(message.getType() == MessageType.PRIVATE){
                    String recipient = message.getRecipient();
                    StreamObserver<ChatMessage> recipientObserver = clients.get(recipient);

                    if(recipientObserver == null){
                        responseObserver.onNext(
                                ChatMessage.newBuilder()
                                        .setSender("System")
                                        .setType(MessageType.SYSTEM)
                                        .setContent("User '" + recipient + "' not found")
                                        .build()
                        );
                        return;
                    }
                    System.out.println("[PRIVATE to " + recipient + "] " + message.getSender() + ": "
                            + message.getContent());

                    recipientObserver.onNext(message);  // private msg to the recipient

                    responseObserver.onNext(message);   // confirmation to the sender

                    return;
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

                    Set<String> joinedRooms = userRooms.remove(username);

                    if(joinedRooms != null){
                        for(String room: joinedRooms){
                            Set<String> roomMembers = rooms.get(room);
                            if(roomMembers != null){
                                roomMembers.remove(username);
                                if(roomMembers.isEmpty()){
                                    rooms.remove(room); // remove the empty room
                                }
                            }
                        }
                    }

                    System.out.println(username + " left");
                }

                ChatMessage leaveNotification = ChatMessage.newBuilder()
                        .setType(MessageType.LEAVE)
                        .setSender(username)
                        .setContent("------ " + username + " left the chat -------")
                        .build();

                for(var entry: clients.entrySet()){ // broadcast leave notification to others
                    if(!entry.getKey().equals(username)) {
                        entry.getValue().onNext(leaveNotification);
                    }
                }

                responseObserver.onCompleted();
            }
        };
    }

    private final ConcurrentHashMap<String, StreamObserver<ChatMessage>> clients =
            new ConcurrentHashMap<>();


}
