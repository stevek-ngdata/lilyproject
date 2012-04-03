package org.lilyproject.rowlog.impl;

interface Triggerable {
    void trigger() throws InterruptedException;
}
