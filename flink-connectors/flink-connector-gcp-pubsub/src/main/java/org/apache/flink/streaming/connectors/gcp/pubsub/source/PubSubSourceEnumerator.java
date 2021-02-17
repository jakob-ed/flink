package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** */
public class PubSubSourceEnumerator
        implements SplitEnumerator<PubSubSplit, PubSubEnumeratorCheckpoint> {
    private final SplitEnumeratorContext<PubSubSplit> context;
    private final String projectSubscriptionName;

    public PubSubSourceEnumerator(
            SplitEnumeratorContext<PubSubSplit> context, String projectSubscriptionName) {
        this.context = context;
        this.projectSubscriptionName = projectSubscriptionName;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    @Override
    public void addSplitsBack(List<PubSubSplit> splits, int subtaskId) {}

    @Override
    public void addReader(int subtaskId) {
        context.assignSplit(new PubSubSplit(projectSubscriptionName), subtaskId);
    }

    @Override
    public PubSubEnumeratorCheckpoint snapshotState() throws Exception {
        return new PubSubEnumeratorCheckpoint();
    }

    @Override
    public void close() throws IOException {}
}
