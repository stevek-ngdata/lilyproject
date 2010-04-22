package org.lilycms.queue.mock;

import org.lilycms.queue.api.LilyQueue;
import org.lilycms.queue.api.QueueListener;
import org.lilycms.queue.api.QueueMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestLilyQueue implements LilyQueue {
    private List<QueueListener> listeners = new ArrayList<QueueListener>();
    private Map<String, QueueMessage> messages = new HashMap<String, QueueMessage>();
    private int counter = 0;

    public QueueMessage getMessage(String id) {
        return messages.get(id);
    }

    public void addListener(String listenerName, QueueListener listener) {
        listeners.add(listener);
    }

    public void removeListener(QueueListener listener) {
        listeners.remove(listener);
    }

    public void broadCastMessage(QueueMessage msg) {
        String msgId = String.valueOf(++counter);
        messages.put(msgId, msg);

        for (QueueListener listener : listeners){
            listener.processMessage(msgId);
        }
    }
}
