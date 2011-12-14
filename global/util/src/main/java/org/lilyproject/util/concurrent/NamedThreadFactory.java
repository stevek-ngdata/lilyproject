package org.lilyproject.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

// Disclaimer: I looked at DefaultThreadFactory while implementing this

public class NamedThreadFactory implements ThreadFactory {
    private final String name;
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private boolean daemon;

    public NamedThreadFactory(String name) {
        this(name, Thread.currentThread().getThreadGroup());
    }

    public NamedThreadFactory(String name, ThreadGroup group) {
        this(name, group, false);
    }
    
    public NamedThreadFactory(String name, ThreadGroup group, boolean daemon) {
        this.name = name;
        this.group = group == null ? Thread.currentThread().getThreadGroup() : null;
        this.namePrefix = name + "-thread-";
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