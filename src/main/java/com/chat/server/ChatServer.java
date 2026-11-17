package com.chat.server;

import com.chat.service.ChatServiceImpl;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ChatServer {

    private static final int SERVER_PORT = 9090;

    public static void main(String[] args) throws IOException, InterruptedException {

        Server server = ServerBuilder.forPort(SERVER_PORT)
                .addService(new ChatServiceImpl())
                .build();

        server.start();
        System.out.println("gRPC server started on port " + SERVER_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            server.shutdown();
            try {
                server.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Server stopped");
        }));

        server.awaitTermination();

    }
}
