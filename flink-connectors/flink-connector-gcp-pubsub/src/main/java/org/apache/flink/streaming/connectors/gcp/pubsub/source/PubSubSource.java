/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.gcp.pubsub.DefaultPubSubSubscriberFactory;
import org.apache.flink.streaming.connectors.gcp.pubsub.DeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubSubscriberFactory;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.enumerator.PubSubEnumeratorCheckpoint;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.enumerator.PubSubEnumeratorCheckpointSerializer;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.enumerator.PubSubSourceEnumerator;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.reader.PubSubRecordEmitter;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.reader.PubSubSourceReader;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.reader.PubSubSplitReader;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.split.PubSubSplit;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.split.PubSubSplitSerializer;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.function.Supplier;

import static com.google.cloud.pubsub.v1.SubscriptionAdminSettings.defaultCredentialsProviderBuilder;

/**
 * A source implementation to pull messages from GCP Pub/Sub into Flink. A {@link PubSubSource} can
 * be constructed through the {@link PubSubSourceBuilder} like so:
 *
 * <pre>{@code
 * PubSubSource.newBuilder()
 *         // The deserialization schema to deserialize Pub/Sub messages
 *         .withDeserializationSchema(new SimpleStringSchema())
 *         // The name string of your Pub/Sub project
 *         .withProjectName(PROJECT_NAME)
 *         // The name string of the subscription you would like to receive messages from
 *         .withSubscriptionName(SUBSCRIPTION_NAME)
 *         // An instance of the com.google.auth.Credentials class to authenticate against Google Cloud
 *         .withCredentials(CREDENTIALS)
 *         .withPubSubSubscriberFactory(
 *                 // The maximum number of messages that should be pulled in one go
 *                 3,
 *                 // The timeout after which the reception of a message is deemed a failure
 *                 Duration.ofSeconds(1),
 *                 // The number of times the reception of a message should be retried in case of failure
 *                 10)
 *         .setProps(new Properties())
 *         .build();
 * }</pre>
 *
 * <p>More details can be found at {@link PubSubSourceBuilder}
 *
 * @param <OUT> The output type of the source.
 */
public class PubSubSource<OUT>
        implements Source<OUT, PubSubSplit, PubSubEnumeratorCheckpoint>, ResultTypeQueryable<OUT> {
    protected final PubSubDeserializationSchema<OUT> deserializationSchema;
    protected final PubSubSubscriberFactory pubSubSubscriberFactory;
    private final Properties props;
    private final Credentials credentials;

    PubSubSource(
            PubSubDeserializationSchema<OUT> deserializationSchema,
            PubSubSubscriberFactory pubSubSubscriberFactory,
            Properties props,
            Credentials credentials) {
        this.deserializationSchema = deserializationSchema;
        this.pubSubSubscriberFactory = pubSubSubscriberFactory;
        this.props = props;
        this.credentials = credentials;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, PubSubSplit> createReader(SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<OUT, Long>>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        Supplier<PubSubSplitReader<OUT>> splitReaderSupplier =
                () -> {
                    try {
                        return new PubSubSplitReader<>(
                                deserializationSchema,
                                pubSubSubscriberFactory.getSubscriber(credentials));
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                };
        PubSubRecordEmitter<OUT> recordEmitter = new PubSubRecordEmitter<>();

        return new PubSubSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                toConfiguration(props),
                readerContext);
    }

    @Override
    public SplitEnumerator<PubSubSplit, PubSubEnumeratorCheckpoint> createEnumerator(
            SplitEnumeratorContext<PubSubSplit> enumContext) {
        return new PubSubSourceEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<PubSubSplit, PubSubEnumeratorCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<PubSubSplit> enumContext,
            PubSubEnumeratorCheckpoint checkpoint) {
        return new PubSubSourceEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<PubSubSplit> getSplitSerializer() {
        return new PubSubSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<PubSubEnumeratorCheckpoint>
            getEnumeratorCheckpointSerializer() {
        return new PubSubEnumeratorCheckpointSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    /**
     * Get a builder to build a {@link PubSubSource}.
     *
     * @return A builder for a @{link PubSubSource}.
     */
    public static DeserializationSchemaBuilder newBuilder() {
        return new DeserializationSchemaBuilder();
    }

    /** @param <OUT> */
    public static class PubSubSourceBuilder<OUT>
            implements ProjectNameBuilder<OUT>, SubscriptionNameBuilder<OUT> {
        private final PubSubDeserializationSchema<OUT> deserializationSchema;
        private String projectName;
        private String subscriptionName;

        private PubSubSubscriberFactory pubSubSubscriberFactory;
        private Properties props;
        private Credentials credentials;

        /**
         * Use any {@link DeserializationSchema} to use in the {@link PubSubSource}. The schema will
         * be wrapped automatically for compatibility with the source.
         *
         * @param deserializationSchema The deserialization schema to use.
         */
        private PubSubSourceBuilder(DeserializationSchema<OUT> deserializationSchema) {
            Preconditions.checkNotNull(deserializationSchema);
            this.deserializationSchema = new DeserializationSchemaWrapper<>(deserializationSchema);
        }

        /**
         * Use a {@link PubSubDeserializationSchema} for the {@link PubSubSource}.
         *
         * @param deserializationSchema The deserialization schema to use.
         */
        private PubSubSourceBuilder(PubSubDeserializationSchema<OUT> deserializationSchema) {
            Preconditions.checkNotNull(deserializationSchema);
            this.deserializationSchema = deserializationSchema;
        }

        @Override
        public SubscriptionNameBuilder<OUT> withProjectName(String projectName) {
            Preconditions.checkNotNull(projectName);
            this.projectName = projectName;
            return this;
        }

        @Override
        public PubSubSourceBuilder<OUT> withSubscriptionName(String subscriptionName) {
            Preconditions.checkNotNull(subscriptionName);
            this.subscriptionName = subscriptionName;
            return this;
        }

        public PubSubSourceBuilder<OUT> withCredentials(Credentials credentials) {
            this.credentials = credentials;
            return this;
        }

        public PubSubSourceBuilder<OUT> withPubSubSubscriberFactory(
                PubSubSubscriberFactory pubSubSubscriberFactory) {
            this.pubSubSubscriberFactory = pubSubSubscriberFactory;
            return this;
        }

        public PubSubSourceBuilder<OUT> withPubSubSubscriberFactory(
                int maxMessagesPerPull, Duration perRequestTimeout, int retries) {
            this.pubSubSubscriberFactory =
                    new DefaultPubSubSubscriberFactory(
                            ProjectSubscriptionName.format(projectName, subscriptionName),
                            retries,
                            perRequestTimeout,
                            maxMessagesPerPull);
            return this;
        }

        public PubSubSourceBuilder setProps(Properties props) {
            this.props = props;
            return this;
        }

        public PubSubSource<OUT> build() throws IOException {
            if (credentials == null) {
                credentials = defaultCredentialsProviderBuilder().build().getCredentials();
            }

            if (pubSubSubscriberFactory == null) {
                pubSubSubscriberFactory =
                        new DefaultPubSubSubscriberFactory(
                                ProjectSubscriptionName.format(projectName, subscriptionName),
                                3,
                                Duration.ofSeconds(15),
                                100);
            }

            return new PubSubSource(
                    deserializationSchema, pubSubSubscriberFactory, props, credentials);
        }
    }

    /** Part of {@link PubSubSourceBuilder} to set required fields. */
    public static class DeserializationSchemaBuilder {
        /**
         * Set the DeserializationSchema used to deserialize incoming PubSubMessages. If you want
         * access to meta data of a PubSubMessage use the overloaded
         * withDeserializationSchema({@link PubSubDeserializationSchema}) method instead.
         */
        public <OUT> ProjectNameBuilder<OUT> withDeserializationSchema(
                DeserializationSchema<OUT> deserializationSchema) {
            return new PubSubSourceBuilder<>(deserializationSchema);
        }

        /** Set the DeserializationSchema used to deserialize incoming PubSubMessages. */
        public <OUT> ProjectNameBuilder<OUT> withDeserializationSchema(
                PubSubDeserializationSchema<OUT> deserializationSchema) {
            return new PubSubSourceBuilder<>(deserializationSchema);
        }
    }

    /** Part of {@link PubSubSourceBuilder} to set required fields. */
    public interface ProjectNameBuilder<OUT> {
        /** Set the project name of the subscription to pull messages from. */
        SubscriptionNameBuilder<OUT> withProjectName(String projectName);
    }

    /** Part of {@link PubSubSourceBuilder} to set required fields. */
    public interface SubscriptionNameBuilder<OUT> {
        /** Set the subscription name of the subscription to pull messages from. */
        PubSubSourceBuilder<OUT> withSubscriptionName(String subscriptionName);
    }

    // ----------- private helper methods ---------------

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }
}
