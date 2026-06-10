package com.chat.client;

import com.chat.grpc.ChatServiceGrpc;
import com.chat.grpc.HelloRequest;
import com.chat.grpc.HelloResponse;
import com.chat.grpc.Number;
import com.chat.grpc.SumResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

public class ChatClient {

    public static void main(String[] args) throws InterruptedException {

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

//        Client Streaming RPC

        ChatServiceGrpc.ChatServiceStub asyncStub = ChatServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1); // to wait until the response has arrived
        StreamObserver<SumResponse> responseObserver = new StreamObserver<>() {

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

        requestObserver.onCompleted();

        latch.await(); //wait until the response has arrived then shutdown
        channel.shutdown();
    }
}
