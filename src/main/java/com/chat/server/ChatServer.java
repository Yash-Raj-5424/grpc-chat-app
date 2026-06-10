package com.chat.server;

import com.chat.service.ChatServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class ChatServer {

    public static void main(String[] args) throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(9090)
                .addService(new ChatServiceImpl())
                .build();

        server.start();
        System.out.println("gRPC server started on port 9090");
        server.awaitTermination();
    }
}
