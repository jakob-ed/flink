package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.connector.source.SourceSplit;

/** */
public class PubSubSplit implements SourceSplit {
    public static final String SPLIT_ID = "0";

    private final String subscriptionName;

    public PubSubSplit(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }
}
