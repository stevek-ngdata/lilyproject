package org.lilycms.queue.api;

public interface QueueListener {
    // TODO need explicit confirmation of successful processing?
    void processMessage(String id);
}
