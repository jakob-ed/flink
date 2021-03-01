/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub.benchmark;

// import org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.generator.TupleGenerator;
// import org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.generator.TupleSender;

import org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.emulator.GCloudEmulatorManager;
import org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.emulator.PubsubHelper;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.emulator.GCloudEmulatorManager.getDockerIpAddress;
import static org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.emulator.GCloudEmulatorManager.getDockerPubSubPort;

@CommandLine.Command(
        name = "DataGenerator",
        mixinStandardHelpOptions = true,
        description = "Start the data generator.")
public class DataGenerator implements Callable<Integer> {

    @CommandLine.Option(
            names = "--numTuples",
            required = true,
            description = "The overall number of tuples to send.",
            defaultValue = "1000000")
    private Integer numTuples;

    @CommandLine.Option(
            names = "--sleepTime",
            required = true,
            description = "Time to sleep between tuple send.",
            defaultValue = "10")
    private Long sleepTime;

    @CommandLine.Option(
            names = "--bufferSize",
            required = true,
            description = "Size of tuples buffer.",
            defaultValue = "2000")
    private Integer bufferSize;

    @CommandLine.Option(
            names = "--port",
            required = true,
            description = "The port.",
            defaultValue = "5001")
    private Integer port;

    private static final String PROJECT_NAME = "benchmark-project";
    private static final String TOPIC_NAME = "benchmark-topic";
    private static final String SUBSCRIPTION_NAME = "benchmark-subscription";

    private static ManagedChannel channel = null;
    private static TransportChannelProvider channelProvider = null;
    private static PubsubHelper pubsubHelper;

    //    @Override
    //    public Integer call() {
    //        BlockingQueue<String> buffer = new ArrayBlockingQueue<>(bufferSize);
    //
    //        try {
    //            ServerSocket serverSocket = new ServerSocket(this.port);
    //            serverSocket.setSoTimeout(900000);
    //            System.out.println("Waiting for client on port " + serverSocket.getLocalPort() +
    // "...");
    //            Socket server = serverSocket.accept();
    //            System.out.println("Just connected to " + server.getRemoteSocketAddress());
    //            PrintWriter out = new PrintWriter(server.getOutputStream(), true);
    //
    //            Thread generator = new TupleGenerator(this.numTuples, this.sleepTime, buffer);
    //            generator.start();
    //
    //            Thread sender = new TupleSender(buffer, this.numTuples, out, serverSocket);
    //            sender.start();
    //
    //            generator.join();
    //            sender.join();
    //
    //            return 0;
    //        } catch (Exception e) {
    //            e.printStackTrace();
    //            return 1;
    //        }
    //    }

    private static void before() throws Exception {
        GCloudEmulatorManager.launchDocker();

        pubsubHelper = getPubsubHelper();
        pubsubHelper.createTopic(PROJECT_NAME, TOPIC_NAME);
        pubsubHelper.createSubscription(PROJECT_NAME, SUBSCRIPTION_NAME, PROJECT_NAME, TOPIC_NAME);
    }

    private static void after() throws Exception {
        pubsubHelper.deleteSubscription(PROJECT_NAME, SUBSCRIPTION_NAME);
        pubsubHelper.deleteTopic(PROJECT_NAME, TOPIC_NAME);

        channel.shutdownNow();
        channel.awaitTermination(1, TimeUnit.MINUTES);
        channel = null;

        GCloudEmulatorManager.terminateDocker();
    }

    @Override
    public Integer call() {
        try {
            before();

            Publisher publisher = pubsubHelper.createPublisher(PROJECT_NAME, TOPIC_NAME);
            try {
                publisher
                        .publish(
                                PubsubMessage.newBuilder()
                                        .setData(ByteString.copyFromUtf8("hello world"))
                                        .build())
                        .get();
                System.out.println("Publication complete?!");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            after();
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    public static PubsubHelper getPubsubHelper() {
        if (channel == null) {
            //noinspection deprecation
            channel = ManagedChannelBuilder.forTarget(getPubSubHostPort()).usePlaintext().build();
            channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        }
        return new PubsubHelper(channelProvider);
    }

    public static String getPubSubHostPort() {
        return getDockerIpAddress() + ":" + getDockerPubSubPort();
    }
}
