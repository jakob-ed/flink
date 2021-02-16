package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** */
public class PubSubSplitSerializer implements SimpleVersionedSerializer<PubSubSplit> {
    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PubSubSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.getSubscriptionName());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PubSubSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String subscriptionName = in.readUTF();
            return new PubSubSplit(subscriptionName);
        }
    }
}
