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
package org.lilyproject.avro;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.avro.ipc.NettyTransceiver;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.lilyproject.util.concurrent.CustomThreadFactory;

public class NettyTransceiverFactory {

    public static NettyTransceiver create(InetSocketAddress address) throws IOException {
        return new NettyTransceiver(address, new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(new DaemonThreadFactory(new CustomThreadFactory("avro-client-boss"))),
                Executors.newCachedThreadPool(new DaemonThreadFactory(new CustomThreadFactory("avro-client-worker")))));
    }

    private static class DaemonThreadFactory implements ThreadFactory {
        private ThreadFactory delegate;

        public DaemonThreadFactory() {
            this.delegate = Executors.defaultThreadFactory();
        }

        public DaemonThreadFactory(ThreadFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = delegate.newThread(r);
            // Using daemon threads so that client applications would exit without having to properly
            // close the RemoteRepository.
            thread.setDaemon(true);
            return thread;
        }
    }

}
