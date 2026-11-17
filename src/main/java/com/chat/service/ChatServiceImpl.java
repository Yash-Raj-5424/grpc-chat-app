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
    public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver){

        return new StreamObserver<>(){

            private String username;

            @Override
            public void onNext(ChatMessage message){
                if (username == null && message.getType() != MessageType.JOIN){
                    sendSystem(responseObserver, "Join first before sending messages");
                    return;
                }
                switch (message.getType()){
                    case JOIN -> {
                        if (username != null){  // already joined on this stream
                            sendSystem(responseObserver, "You are already joined as '" + username + "'");
                            return;
                        }
                        username = handleJoin(message, responseObserver);
                    }
                    case ROOM_JOIN -> handleRoomJoin(message, username, responseObserver);
                    case ROOM_LEAVE -> handleRoomLeave(message, username, responseObserver);
                    case ROOM_CHAT -> handleRoomChat(message, username, responseObserver);
                    case PRIVATE -> handlePrivateChat(message, username, responseObserver);
                    case CHAT -> handleGlobalChat(message, username, responseObserver);
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
            String username,
            StreamObserver<ChatMessage> responseObserver
    ){
        String room = message.getRoom();

        // check membership on server-side
        Set<String> joinedRooms = userRooms.get(username);
        Set<String> roomMembers = rooms.get(room);
        if (joinedRooms == null || !joinedRooms.contains(room) || roomMembers == null){
            sendSystem(responseObserver, "You are not in room: '" + room + "'");
            return;
        }

        System.out.println("[ROOM: " + room + "] " + username + ": " + message.getContent());

        ChatMessage roomMessage = ChatMessage.newBuilder()
                .setSender(username)
                .setRoom(room)
                .setType(MessageType.ROOM_CHAT)
                .setContent(message.getContent())
                .build();

        for (String member : roomMembers){
            StreamObserver<ChatMessage> observer = clients.get(member);
            if (observer != null) {
                send(member, observer, roomMessage);
            }
        }
    }

    private void handleGlobalChat(
            ChatMessage message,
            String username,
            StreamObserver<ChatMessage> responseObserver
    ){
        System.out.println(username + ": " + message.getContent());

        ChatMessage globalMessage = ChatMessage.newBuilder()
                .setSender(username)
                .setType(MessageType.CHAT)
                .setContent(message.getContent())
                .build();

        for (var entry : clients.entrySet()){
            send(entry.getKey(), entry.getValue(), globalMessage);
        }
    }

    private void handlePrivateChat(
            ChatMessage message,
            String username,
            StreamObserver<ChatMessage> responseObserver
    ){
        String recipient = message.getRecipient();
        StreamObserver<ChatMessage> recipientObserver = clients.get(recipient);

        if (recipientObserver == null){
            sendSystem(responseObserver, "User '" + recipient + "' not found");
            return;
        }

        System.out.println("[PRIVATE to " + recipient + "] " + username + ": " + message.getContent());

        ChatMessage privateMessage = ChatMessage.newBuilder()
                .setSender(username)
                .setRecipient(recipient)
                .setType(MessageType.PRIVATE)
                .setContent(message.getContent())
                .build();

        send(recipient, recipientObserver, privateMessage);  // private msg to the recipient
        responseObserver.onNext(privateMessage);   // confirmation to the sender
    }

    private void send(
            String username,
            StreamObserver<ChatMessage> observer,
            ChatMessage message
    ){
        try {
            observer.onNext(message);
        } catch (Exception e) {
            clients.remove(username);
            userRooms.remove(username);
            System.out.println(username + " dropped: " + e.getMessage());
        }
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
                send(entry.getKey(), entry.getValue(), message);
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
