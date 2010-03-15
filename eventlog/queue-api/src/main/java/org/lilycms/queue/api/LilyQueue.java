package org.lilycms.queue.api;

public interface LilyQueue {
    void addListener(String listenerName, QueueListener listener);

    void removeListener(QueueListener listener);

    // TODO the idea of having to retrieve the message separately is so that the data
    // should not pass through the 'queue manager'
    QueueMessage getMessage(String id);
}
