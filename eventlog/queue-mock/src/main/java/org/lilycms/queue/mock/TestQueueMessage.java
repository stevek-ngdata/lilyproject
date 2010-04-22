package org.lilycms.queue.mock;

import org.lilycms.queue.api.QueueMessage;
import org.lilycms.repository.api.RecordId;

public class TestQueueMessage implements QueueMessage {
    private String type;
    private RecordId recordId;
    private byte[] data;

    public TestQueueMessage(String type, RecordId recordId, byte[] data) {
        this.type = type;
        this.recordId = recordId;
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public byte[] getData() {
        return data;
    }
}
