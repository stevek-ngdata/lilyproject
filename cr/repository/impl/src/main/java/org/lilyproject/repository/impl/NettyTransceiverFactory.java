package org.lilyproject.repository.impl;

import org.apache.avro.ipc.NettyTransceiver;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.lilyproject.util.concurrent.NamedThreadFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class NettyTransceiverFactory {

    public static NettyTransceiver create(InetSocketAddress address) {
        return new NettyTransceiver(address, new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(new DaemonThreadFactory(new NamedThreadFactory("avro-client-boss"))),
                Executors.newCachedThreadPool(new DaemonThreadFactory(new NamedThreadFactory("avro-client-worker")))));
    }

    private static class DaemonThreadFactory implements ThreadFactory {
        private ThreadFactory delegate;

        public DaemonThreadFactory() {
            this.delegate = Executors.defaultThreadFactory();
        }

        public DaemonThreadFactory(ThreadFactory delegate) {
            this.delegate = delegate;
        }

        public Thread newThread(Runnable r) {
            Thread thread = delegate.newThread(r);
            // Using daemon threads so that client applications would exit without having to properly
            // close the RemoteRepository.
            thread.setDaemon(true);
            return thread;
        }
    }

}
