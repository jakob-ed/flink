# Flink Source for Google Cloud Pub/Sub

This is a source implementation for receiving Google Cloud Pub/Sub messages in Flink with an at-least-once guarantee.

## Installation

Add this dependency entry to your pom.xml to use the Google Cloud Pub/Sub source:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-gcp-pubsub_2.11</artifactId>
  <version>1.13-SNAPSHOT</version>
</dependency>
```

## Usage

Please keep in mind that the new source can be found in package `org.apache.flink.streaming.connectors.gcp.pubsub.source` while the old source implementation can still be found in `org.apache.flink.streaming.connectors.gcp.pubsub` (without `source` suffix). The API of the new source is not much different from the old one.

To keep up the Google Cloud Pub/Sub at-least-once guarantee, messages are acknowledged against Pub/Sub when checkpointing succeeds. If a message is not acknowledged within a timeout (here `Duration.ofSeconds(1)`), Pub/Sub will attempt redelivery. To avoid unnecessary redelivery of successfully received messages, the timeout after which the reception of a message is deemed a failure (acknowledge deadline) should always be configured (much) *higher* than the checkpointing interval!

```java
import org.apache.flink.streaming.connectors.gcp.pubsub.source.PubSubSource;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Checkpointing must be enabled for the source to work so that messages can be acknowledged towards Pub/Sub
env.enableCheckpointing(1000);

// Parallelism > 1 may be set
// env.setParallelism(4);

PubSubSource<String> source =
        PubSubSource.newBuilder()
                // The deserialization schema to deserialize Pub/Sub messages
                .withDeserializationSchema(new SimpleStringSchema())
                // The name string of your Pub/Sub project
                .withProjectName(PROJECT_NAME)
                // The name string of the subscription you would like to receive messages from
                .withSubscriptionName(SUBSCRIPTION_NAME)
                // An instance of the com.google.auth.Credentials class to authenticate against Google Cloud
                .withCredentials(CREDENTIALS)
                .withPubSubSubscriberFactory(
                        // The maximum number of messages that should be pulled in one go
                        3,
                        // The timeout after which the reception of a message is deemed a failure
                        Duration.ofSeconds(1),
                        // The number of times the reception of a message should be retried in case of failure
                        10)
                .setProps(new Properties())
                .build();

DataStream<String> fromPubSub =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub-source");
```

## Internals

#### Split Enumerator

Pub/Sub doesn't expose any partitions to consuming applications. Therefore, the implementation of the `PubSubSourceEnumerator` doesn't do any real work discovery. Instead, a static `PubSubSplit` is handed to every `PubSubSourceReader` which requests a source split. This static source split doesn't contain split-specific information like partitions/offsets because this information can not be obtained.

#### Source Reader

A `PubSubSourceReader` uses Pub/Sub's pull mechanism to read new messages from the Pub/Sub subscription specified by the user. In the case of parallel-running source readers in Flink, every source reader is passed the same source split from the enumerator. Because of this, all source readers use the same connection details and the same Pub/Sub subscription to receive messages. In this case, Pub/Sub automatically load-balances messages between all source readers which pull from the same subscription. This way, parallel processing is achieved in the source.

#### At-least-once guarantee

Pub/Sub only guarantees at-least-once message delivery. This guarantee is kept up by the source as well. The mechanism that is used to achieve this is that Pub/Sub expects a message to be acknowledged by the subscriber to signal that the message has been consumed successfully. Any message that has not been acknowledged yet will be automatically redelivered by Pub/Sub once an ack deadline has passed.

When a new checkpoint is written...
- all messages pulled since the previous checkpoint are acknowledged to Pub/Sub and...
- are forwarded to down-stream tasks

This ensures at-least-once delivery in the source because in the case of failure, non-checkpointed messages have not yet been acknowledged and will therefore be redelivered by the Pub/Sub service. Because of this automatic redelivery and given at-least-once guarantee, it's also not necessary to write any data such as message IDs into the checkpoint.

Because of the static source split, no checkpointing is necessary in the enumerator either.
