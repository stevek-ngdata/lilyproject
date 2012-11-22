package org.lilyproject.sep.impl;

public interface EventListener {
    boolean processMessage(byte[] row, byte[] payload);
}
