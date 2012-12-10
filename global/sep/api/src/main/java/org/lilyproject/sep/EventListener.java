package org.lilyproject.sep;

/**
 * Handles incoming Secondary Event Processor messages.
 */
public interface EventListener {
    boolean processMessage(byte[] row, byte[] payload);
}
