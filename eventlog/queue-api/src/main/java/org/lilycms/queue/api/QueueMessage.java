package org.lilycms.queue.api;

import org.lilycms.repository.api.RecordId;

import java.util.Map;

/**
 * A message from the message queue.
 */
public interface QueueMessage {
    String getType();

    RecordId getRecordId();

    byte[] getData();
}
