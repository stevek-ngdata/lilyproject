package org.lilyproject.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

// Disclaimer: I looked at DefaultThreadFactory while implementing this

public class CustomThreadFactory implements ThreadFactory {
    private final String name;
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private boolean daemon;

    public CustomThreadFactory(String name) {
        this(name, Thread.currentThread().getThreadGroup());
    }

    public CustomThreadFactory(String name, ThreadGroup group) {
        this(name, group, false);
    }
    
    public CustomThreadFactory(String name, ThreadGroup group, boolean daemon) {
        this.name = name;
        this.group = group == null ? Thread.currentThread().getThreadGroup() : group;
        this.namePrefix = name + "-thread-";
        this.daemon = daemon;
    }
    
    @Override    
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);

        if (daemon)
            t.setDaemon(true);
        else if (t.isDaemon())
            t.setDaemon(false);

        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);

        return t;
    }
}