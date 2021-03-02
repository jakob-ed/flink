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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.EmulatorCredentials;
import org.apache.flink.streaming.connectors.gcp.pubsub.emulator.PubSubSubscriberFactoryForEmulator;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.PubSubSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import picocli.CommandLine;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "FlinkApp", description = "Starts the benchmark app.")
public class FlinkApp implements Callable<Integer> {

    @CommandLine.Option(
            names = "--parallelism",
            required = true,
            description = "The num of partitions to execute the Flink application with.",
            defaultValue = "1")
    private Integer parallelism;

    //    @CommandLine.Option(
    //            names = "--segmentSize",
    //            required = true,
    //            description = "The number of tuples per segment.")
    //    private Integer segmentSize;

    @CommandLine.Option(
            names = "--checkpointingInterval",
            required = true,
            description = "The Flink checkpointing interval.",
            defaultValue = "100")
    private Long checkpointingInterval;

    @CommandLine.Option(
            names = "--host",
            required = true,
            description = "The Pub/Sub host.",
            defaultValue = "127.0.0.1")
    private String host;

    @CommandLine.Option(
            names = "--port",
            required = true,
            description = "The Pub/Sub port.",
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
            description = "Startup delay.",
            defaultValue = "60000")
    private Long delay;

    @Override
    public Integer call() throws Exception {
        System.out.println("started");
        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(this.parallelism);
        env.enableCheckpointing(this.checkpointingInterval);
        //        TODO: ???
        //        env.setRestartStrategy(RestartStrategies.noRestart());
        //        TODO: this enough or need to set config on env again?
        env.getConfig().enableObjectReuse();

        ObjectMapper objectMapper = new ObjectMapper();

        // TODO: use ..<BenchmarkEvent> ?
        PubSubSource<String> source =
                PubSubSource.newBuilder()
                        // TODO: different schema? how to do deserialization efficiently? avro?
                        .withDeserializationSchema(new SimpleStringSchema())
                        .withProjectName(project)
                        .withSubscriptionName(subscription)
                        .withCredentials(EmulatorCredentials.getInstance())
                        .withPubSubSubscriberFactory(
                                new PubSubSubscriberFactoryForEmulator(
                                        getPubSubHostPort(),
                                        project,
                                        subscription,
                                        // 10,
                                        2,
                                        Duration.ofSeconds(1), // timeout
                                        3))
                        // TODOI: necessary?
                        .setProps(new Properties())
                        .build();

        DataStream<Integer> dataStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub-source")
                        .map(message -> objectMapper.readValue(message, BenchmarkEvent.class))
                        .map(
                                message -> {
                                    System.out.println("received " + message);
                                    return message;
                                })
                        .map(message -> 1)
                        .timeWindowAll(Time.seconds(5))
                        .reduce(Integer::sum)
                        .map(
                                message -> {
                                    System.out.println("current count " + message);
                                    return message;
                                });

        //        this.setupSource(env)
        //                .map(message -> JsonUtils.stringToObject(message, BenchmarkEvent.class))
        //                .map(
        //                        message -> {
        //                            message.setFlinkSourceTime(System.currentTimeMillis());
        //                            return message;
        //                        });
        //        this.setupSink(dataStream);

        // Wait before trying to connect to the Pinot controller
        //        Thread.sleep(delay);

        //        this.setupPinot();

        System.out.println("executing...");
        env.execute();
        System.out.println("executed");
        return 0;
    }

    //    private DataStream<String> setupSource(StreamExecutionEnvironment env) {
    //        DataStream<String> socketSource = null;
    //        for (String host : Collections.singletonList("data-generator")) {
    //            for (int port : Collections.singletonList(this.port)) {
    //                DataStream<String> socketSource_i = env.socketTextStream(host, port);
    //                socketSource =
    //                        socketSource == null ? socketSource_i :
    // socketSource.union(socketSource_i);
    //            }
    //        }
    //
    //        return socketSource;
    //    }
    //
    //    private void setupSink(DataStream<BenchmarkEvent> dataStream) {
    //        SegmentNameGenerator segmentNameGenerator =
    //                new PinotSegmentNameGenerator(PinotTableConfig.TABLE_NAME, "flink-connector");
    //        FileSystemAdapter fsAdapter = new
    // LocalFileSystemAdapter("flink-pinot-connector-benchmark");
    //
    //        BenchmarkEventTimeExtractor eventTimeExtractor = new BenchmarkEventTimeExtractor();
    //        dataStream
    //                .sinkTo(
    //                        new PinotSink<>(
    //                                PINOT_CONTROLLER_HOST,
    //                                PINOT_CONTROLLER_PORT,
    //                                PinotTableConfig.TABLE_NAME,
    //                                segmentSize,
    //                                eventTimeExtractor,
    //                                segmentNameGenerator,
    //                                fsAdapter))
    //                .name("Pinot Sink");
    //    }
    //
    //    private void setupPinot() throws IOException {
    //        PinotHelper pinotHelper = new PinotHelper(PINOT_CONTROLLER_HOST,
    // PINOT_CONTROLLER_PORT);
    //        pinotHelper.createTable(
    //                PinotTableConfig.getTableConfig(), PinotTableConfig.getTableSchema());
    //    }
    //
    //    static class PinotTableConfig {
    //
    //        static final String TABLE_NAME = "BenchmarkTable";
    //        static final String SCHEMA_NAME = "BenchmarkTableSchema";
    //
    //        private static SegmentsValidationAndRetentionConfig getValidationConfig() {
    //            SegmentsValidationAndRetentionConfig validationConfig =
    //                    new SegmentsValidationAndRetentionConfig();
    //
    // validationConfig.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
    //            validationConfig.setSegmentPushType("APPEND");
    //            validationConfig.setSchemaName(SCHEMA_NAME);
    //            validationConfig.setReplication("1");
    //            return validationConfig;
    //        }
    //
    //        private static TenantConfig getTenantConfig() {
    //            TenantConfig tenantConfig = new TenantConfig("DefaultTenant", "DefaultTenant",
    // null);
    //            return tenantConfig;
    //        }
    //
    //        private static IndexingConfig getIndexingConfig() {
    //            IndexingConfig indexingConfig = new IndexingConfig();
    //            return indexingConfig;
    //        }
    //
    //        private static TableCustomConfig getCustomConfig() {
    //            TableCustomConfig customConfig = new TableCustomConfig(null);
    //            ;
    //            return customConfig;
    //        }
    //
    //        static TableConfig getTableConfig() {
    //            return new TableConfig(
    //                    TABLE_NAME,
    //                    TableType.OFFLINE.name(),
    //                    getValidationConfig(),
    //                    getTenantConfig(),
    //                    getIndexingConfig(),
    //                    getCustomConfig(),
    //                    null,
    //                    null,
    //                    null,
    //                    null,
    //                    null,
    //                    null,
    //                    null,
    //                    null,
    //                    null);
    //        }
    //
    //        static Schema getTableSchema() {
    //            Schema schema = new Schema();
    //            schema.setSchemaName(SCHEMA_NAME);
    //            schema.addField(new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true));
    //            schema.addField(new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true));
    //            schema.addField(new DimensionFieldSpec("eventTime", FieldSpec.DataType.LONG,
    // true));
    //            schema.addField(
    //                    new DimensionFieldSpec("flinkSourceTime", FieldSpec.DataType.LONG, true));
    //            return schema;
    //        }
    //    }

    public String getPubSubHostPort() {
        return host + ":" + port;
    }
}
