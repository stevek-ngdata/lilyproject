/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
