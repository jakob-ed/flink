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

package org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.generator;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class TupleSender extends Thread {
    private final BlockingQueue<String> buffer;
    private final Logger logger = Logger.getLogger("MyLog");
    private final Publisher publisher;
    private final int tupleCount;
    private final HashMap<Long, Integer> thoughputCount = new HashMap<>();

    public TupleSender(BlockingQueue<String> buffer, int tupleCount, Publisher publisher) {
        this.buffer = buffer;
        this.publisher = publisher;
        this.tupleCount = tupleCount;
    }

    public void run() {
        try {
            long timeStart = System.currentTimeMillis();

            int tempVal = 0;
            for (int i = 0; i < tupleCount; i++) {
                String tuple = buffer.take();

                try {
                    publisher
                            .publish(
                                    PubsubMessage.newBuilder()
                                            .setData(ByteString.copyFromUtf8(tuple))
                                            .build())
                            .get();
                    logger.warning("Publication complete");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }

                if (i % 1000 == 0) {
                    thoughputCount.put(
                            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                            i - tempVal);
                    tempVal = i;
                    logger.info(i + " tuples sent from buffer");
                }
            }
            long timeEnd = System.currentTimeMillis();
            long runtime = (timeEnd - timeStart) / 1000;
            long throughput = this.tupleCount / runtime;

            logger.info(
                    "---BENCHMARK ENDED--- on "
                            + runtime
                            + " seconds with "
                            + throughput
                            + " throughput "
                            + " node : "
                            + InetAddress.getLocalHost().getHostName());
            //            logger.info("Waiting for client on port " + serverSocket.getLocalPort() +
            // "...");
            //            Socket server = serverSocket.accept();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
