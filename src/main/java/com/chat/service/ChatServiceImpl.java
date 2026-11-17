package com.chat.service;

import com.chat.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {

    private final ConcurrentHashMap<String, Set<String>> rooms = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> userRooms = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StreamObserver<ChatMessage>> clients =
            new ConcurrentHashMap<>();

    @Override
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver) {

        return new StreamObserver<>() {

            private String username;

            @Override
            public void onNext(ChatMessage message){
                switch (message.getType()){
                    case JOIN -> username = handleJoin(message, responseObserver);
                    case ROOM_JOIN -> handleRoomJoin(message, username, responseObserver);
                    case ROOM_LEAVE -> handleRoomLeave(message, username, responseObserver);
                    case ROOM_CHAT -> handleRoomChat(message, responseObserver);
                    case PRIVATE -> handlePrivateChat(message, responseObserver);
                    case CHAT -> handleGlobalChat(message);
                    default -> sendSystem(responseObserver, "Unknown message type: "
                            + message.getType());
                }
            }

            @Override
            public void onError(Throwable throwable){
                if (username != null) {
                    clients.remove(username);
                    System.out.println(username + " disconnected: " + throwable.getMessage());
                }
            }

            @Override
            public void onCompleted(){
                if (username != null){
                    leaveAllRooms(username);
                    clients.remove(username);
                    System.out.println(username + " left");
                }
                broadcastLeaveNotification(username);
                responseObserver.onCompleted();
            }
        };
    }

    private String handleJoin(
            ChatMessage message,
            StreamObserver<ChatMessage> responseObserver
    ){
        String username = message.getSender();

        StreamObserver<ChatMessage> existing = clients.putIfAbsent(username, responseObserver);
        if (existing != null){   // prevent duplicate usernames
            sendSystem(responseObserver, "Username: '" + username
                    + "' is already taken. Reconnect and try a different one.");
            responseObserver.onCompleted(); // close the stream
            return null;
        }

        sendSystem(responseObserver, "JOIN_SUCCESS"); // client sees join success

        System.out.println(username + " joined");

        ChatMessage joinNotification = ChatMessage.newBuilder()
                .setType(MessageType.JOIN)
                .setSender(username)
                .setContent("------ " + username + " joined the chat -------")
                .build();

        broadcastToOthers(joinNotification, username);
        return username;
    }

    private void handleRoomJoin(
            ChatMessage message,
            String username,
            StreamObserver<ChatMessage> responseObserver
    ){
        String room = message.getRoom();

        // room name validation
        if (room.contains(" ")){
            System.out.println("Invalid room name: '" + room + "'. Room names cannot contain spaces");
            sendSystem(responseObserver, "Invalid room name: '"
                    + room + "'. Room names cannot contain spaces");
            return;
        }

        rooms.computeIfAbsent(room, k -> ConcurrentHashMap.newKeySet())
                .add(username);
        userRooms.computeIfAbsent(username, k -> ConcurrentHashMap.newKeySet())
                .add(room);

        System.out.println(username + " joined room: " + room);

        sendSystem(responseObserver, "Joined room: '" + room + "'");
    }

    private void handleRoomLeave(
            ChatMessage message,
            String username,
            StreamObserver<ChatMessage> responseObserver
    ){
        String room = message.getRoom();
        Set<String> joinedRooms = userRooms.get(username);

        if (joinedRooms == null || !joinedRooms.contains(room)){
            sendSystem(responseObserver, "You are not in room: '" + room + "'");
            return;
        }

        leaveRoom(username, room);

        System.out.println(username + " left room: '" + room + "'");

        sendSystem(responseObserver, "Left room: '" + room + "'");
    }

    private void handleRoomChat(
            ChatMessage message,
            StreamObserver<ChatMessage> responseObserver
    ){
        String room = message.getRoom();
        Set<String> roomMembers = rooms.get(room);

        if (roomMembers == null) {
            sendSystem(responseObserver, "Room '" + room + "' does not exist");
            return;
        }

        System.out.println("[ROOM: " + room + "] " + message.getSender() + ": "
                + message.getContent());

        for (String member : roomMembers){
            StreamObserver<ChatMessage> observer = clients.get(member);
            if (observer != null) {
                observer.onNext(message);
            }
        }
    }

    private void handleGlobalChat(ChatMessage message){  // broadcast messaging
        System.out.println(message.getSender() + ": " + message.getContent());

        for (StreamObserver<ChatMessage> observer : clients.values()){
            observer.onNext(message);
        }
    }

    private void handlePrivateChat(
            ChatMessage message,
            StreamObserver<ChatMessage> responseObserver
    ){
        String recipient = message.getRecipient();
        StreamObserver<ChatMessage> recipientObserver = clients.get(recipient);

        if (recipientObserver == null){
            sendSystem(responseObserver, "User '" + recipient + "' not found");
            return;
        }

        System.out.println("[PRIVATE to " + recipient + "] " + message.getSender() + ": "
                + message.getContent());

        recipientObserver.onNext(message);  // private msg to the recipient
        responseObserver.onNext(message);   // confirmation to the sender
    }

    private void sendSystem(
            StreamObserver<ChatMessage> responseObserver,
            String content
    ){
        responseObserver.onNext(
                ChatMessage.newBuilder()
                        .setSender("System")
                        .setType(MessageType.SYSTEM)
                        .setContent(content)
                        .build()
        );
    }

    private void broadcastToOthers(ChatMessage message, String sender){
        for (var entry : clients.entrySet()){
            if (!entry.getKey().equals(sender)){
                entry.getValue().onNext(message);
            }
        }
    }

    private void broadcastLeaveNotification(String username){
        ChatMessage leaveNotification = ChatMessage.newBuilder()
                .setType(MessageType.LEAVE)
                .setSender(username)
                .setContent("------ " + username + " left the chat -------")
                .build();

        broadcastToOthers(leaveNotification, username);
    }

    private void leaveAllRooms(String username){
        Set<String> joinedRooms = userRooms.remove(username);
        if (joinedRooms == null){
            return;
        }
        for (String room : joinedRooms){
            leaveRoom(username, room);
        }
    }

    private void leaveRoom(String username, String room){
        Set<String> roomMembers = rooms.get(room);
        Set<String> joinedRooms = userRooms.get(username);

        if (roomMembers != null){
            roomMembers.remove(username);
            if (roomMembers.isEmpty()){
                rooms.remove(room); // remove the empty room
            }
        }

        if (joinedRooms != null){
            joinedRooms.remove(room);
            if (joinedRooms.isEmpty()) {
                userRooms.remove(username);
            }
        }
    }
}
