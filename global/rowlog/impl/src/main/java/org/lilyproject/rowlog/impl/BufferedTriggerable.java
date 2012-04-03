package org.lilyproject.rowlog.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Buffers up triggers during a certain interval and only lets one go through. This is useful
 * when triggers are produced at a high rate.
 *
 * <p>This is an active component which needs to be stopped when it is not longer used,
 * see {@link #stop}.</p>
 */
public class BufferedTriggerable implements Triggerable {
    enum Mode { BUFFERING, SLEEPING }
    private long delay;
    private Mode mode = Mode.SLEEPING;
    private final Object modeLock = new Object();
    private final Object condition = new Object();
    private Triggerable delegate;
    private Thread thread;
    private Log log = LogFactory.getLog(getClass());
    
    public BufferedTriggerable(Triggerable delegate, long delay) {
        this.delegate = delegate;
        this.delay = delay;
        this.thread = new Thread(new TriggerFlusher());
        thread.start();
    }

    public void stop() throws InterruptedException {
        this.thread.interrupt();
        this.thread.join();
    }
    
    public void trigger() {
        synchronized (modeLock) {
            if (mode == Mode.SLEEPING) {
                mode = Mode.BUFFERING;
                synchronized (condition) {
                    condition.notify();
                }
            }
        }
    }
    
    private class TriggerFlusher implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    synchronized (condition) {
                        while (mode == Mode.SLEEPING) {
                            condition.wait();
                        }
                    }

                    if (mode == Mode.BUFFERING) {
                        Thread.sleep(delay);
                        synchronized (modeLock) {
                            mode = Mode.SLEEPING;
                        }
                        delegate.trigger();
                    }
                } catch (InterruptedException e) {
                    // we are asked to stop
                    return;
                } catch (Throwable t) {
                    log.error("Error in " + getClass().getSimpleName(), t);
                }
            }
        }
    }
}
