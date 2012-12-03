package org.lilyproject.sep.impl;

/**
 * Handles incoming Secondary Event Processor messages.
 */
public interface EventListener {
    boolean processMessage(byte[] row, byte[] payload);
}
