package org.apache.flink.streaming.connectors.gcp.pubsub.source;

/** */
public class PubSubSplitState extends PubSubSplit {
    //    private final String projectSubscriptionName;

    public PubSubSplitState(String projectSubscriptionName) {
        super(projectSubscriptionName);
        //        this.projectSubscriptionName = projectSubscriptionName;
    }
}
