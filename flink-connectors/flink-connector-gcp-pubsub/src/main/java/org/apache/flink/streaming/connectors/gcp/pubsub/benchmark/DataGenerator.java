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

import org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.emulator.PubsubHelper;
import org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.generator.TupleGenerator;
import org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.generator.TupleSender;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(
        name = "DataGenerator",
        mixinStandardHelpOptions = true,
        description = "Start the data generator.")
public class DataGenerator implements Callable<Integer> {

    @CommandLine.Option(
            names = "--numTuples",
            required = true,
            description = "The overall number of tuples to send.",
            defaultValue = "100")
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
            defaultValue = "10")
    private Integer bufferSize;

    @CommandLine.Option(
            names = "--host",
            required = true,
            description = "The host.",
            defaultValue = "127.0.0.1")
    private String host;

    @CommandLine.Option(
            names = "--port",
            required = true,
            description = "The port.",
            defaultValue = "22222")
    private Integer port;

    @CommandLine.Option(
            names = "--ps-project",
            required = true,
            description = "The Pub/Sub project.",
            defaultValue = "benchmark-project")
    private String project;

    @CommandLine.Option(
            names = "--ps-topic",
            required = true,
            description = "The Pub/Sub topic.",
            defaultValue = "benchmark-topic")
    private String topic;

    @CommandLine.Option(
            names = "--ps-subscription",
            required = true,
            description = "The Pub/Sub project.",
            defaultValue = "benchmark-subscription")
    private String subscription;

    @CommandLine.Option(
            names = "--delay",
            required = true,
            description = "Startup delay in ms.",
            defaultValue = "30000")
    private Long delay;

    private static ManagedChannel channel = null;
    private static TransportChannelProvider channelProvider = null;
    private static PubsubHelper pubsubHelper;

    private void before() throws Exception {
        pubsubHelper = getPubsubHelper();
        pubsubHelper.createTopic(project, topic);
        pubsubHelper.createSubscription(project, subscription, project, topic);
    }

    private void after() throws Exception {
        //        pubsubHelper.deleteSubscription(PROJECT_NAME, SUBSCRIPTION_NAME);
        //        pubsubHelper.deleteTopic(PROJECT_NAME, TOPIC_NAME);

        channel.shutdownNow();
        channel.awaitTermination(1, TimeUnit.MINUTES);
        channel = null;
    }

    @Override
    public Integer call() {
        System.out.println("started");
        BlockingQueue<String> buffer = new ArrayBlockingQueue<>(bufferSize);

        try {
            before();

            Publisher publisher = pubsubHelper.createPublisher(project, topic);

            //                        try {
            //                            publisher
            //                                    .publish(
            //                                            PubsubMessage.newBuilder()
            //
            // .setData(ByteString.copyFromUtf8("hello
            //             world"))
            //                                                    .build())
            //                                    .get();
            //                            System.out.println("Publication complete?!");
            //                        } catch (InterruptedException | ExecutionException e) {
            //                            e.printStackTrace();
            //                        }

            Thread.sleep(delay);

            Thread generator = new TupleGenerator(this.numTuples, this.sleepTime, buffer);
            generator.start();

            Thread sender = new TupleSender(buffer, this.numTuples, publisher);
            sender.start();

            System.out.println("joining");
            generator.join();
            sender.join();

            after();
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    public PubsubHelper getPubsubHelper() {
        if (channel == null) {
            //noinspection deprecation
            channel = ManagedChannelBuilder.forTarget(getPubSubHostPort()).usePlaintext().build();
            channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        }
        return new PubsubHelper(channelProvider);
    }

    public String getPubSubHostPort() {
        return host + ":" + port;
    }
}
