/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.rowlog.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.io.Closer;

public class RemoteListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    private ClientBootstrap bootstrap;
    private NioClientSocketChannelFactory channelFactory;
    private boolean remoteProcessMessageResult = false;
    private boolean exceptionCaught = false;

    private Log log = LogFactory.getLog(getClass());

    public RemoteListenersSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue,
            RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
        initBootstrap();
    }

    /**
     * Processes a message by sending the message to a remote listener.
     * This method retries until a communication channel has been successfully setup and a result has been received
     * from the remote listener.
     */
    protected boolean processMessage(String host, RowLogMessage message) throws InterruptedException {
        remoteProcessMessageResult = false;
        exceptionCaught = false;
        Channel channel = null;
        while (channel == null || (!channel.isConnected())) {
            channel = getListenerChannel(host);
        }

        channel.write(message);
        ChannelFuture closeFuture = channel.getCloseFuture();
        try {
            // When the channel is closed, this means the messages has been
            // processed by the remote listener and a result has been
            // received or an error condition occurred.
            closeFuture.await();
        } catch (InterruptedException e) {
            Closer.close(channel);
            throw e;
        }
        if (exceptionCaught) {
            // Retry
            return processMessage(host, message);
        }
        return remoteProcessMessageResult;
    }

    private void initBootstrap() {
        if (bootstrap == null) {
            if (channelFactory == null) {
                channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                        .newCachedThreadPool());
            }
            bootstrap = new ClientBootstrap(channelFactory);
            bootstrap.setPipelineFactory(new ChannelPipelineFactoryImplementation(this));
            bootstrap.setOption("tcpNoDelay", true);
            bootstrap.setOption("keepAlive", true);
        }
    }

    private Channel getListenerChannel(String host) throws InterruptedException {
        String listenerHostAndPort[] = host.split(":");
        ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(listenerHostAndPort[0], Integer
                .valueOf(listenerHostAndPort[1])));
        connectFuture.await();
        if (connectFuture.isSuccess()) {
            return connectFuture.getChannel();
        } else {
            return null;
        }
    }

    private static final class ChannelPipelineFactoryImplementation implements ChannelPipelineFactory {
        private static final ResultDecoder RESULT_DECODER = new ResultDecoder();
        private static final MessageEncoder MESSAGE_ENCODER = new MessageEncoder();
        private final RemoteListenersSubscriptionHandler remoteListenersSubscriptionHandler;

        public ChannelPipelineFactoryImplementation(
                RemoteListenersSubscriptionHandler remoteListenersSubscriptionHandler) {
                    this.remoteListenersSubscriptionHandler = remoteListenersSubscriptionHandler;
        }

        public ChannelPipeline getPipeline() {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("resultDecoder", RESULT_DECODER);
            pipeline.addLast("resultHandler", remoteListenersSubscriptionHandler.new ResultHandler());
            pipeline.addLast("messageEncoder", MESSAGE_ENCODER);
            return pipeline;
        }
    }

    private static class ResultDecoder extends OneToOneDecoder {
        @Override
        protected Boolean decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            ChannelBufferInputStream inputStream = new ChannelBufferInputStream((ChannelBuffer) msg);
            try {
                return inputStream.readBoolean();
            } finally {
                Closer.close(inputStream);
            }
        }
    }

    private class ResultHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            remoteProcessMessageResult = (Boolean) e.getMessage();
            Closer.close(e.getChannel());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            exceptionCaught = true;
            log.warn("Exception caught in ResultHandler", e.getCause());
            Closer.close(e.getChannel());
        }
    }

    private static class MessageEncoder extends OneToOneEncoder {
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            ChannelBufferOutputStream outputStream = null;
            try {
                RowLogMessage message = (RowLogMessage) msg;
                byte[] rowKey = message.getRowKey();
                byte[] data = message.getData();
                int capacity = 4 + 8 + rowKey.length + 8 + 4;
                if (data != null)
                    capacity = capacity + data.length;
                ChannelBuffer channelBuffer = ChannelBuffers.buffer(capacity);
                outputStream = new ChannelBufferOutputStream(channelBuffer);
                outputStream.writeLong(message.getTimestamp());
                outputStream.writeInt(rowKey.length);
                outputStream.write(rowKey);
                outputStream.writeLong(message.getSeqNr());
                if (data != null) {
                    outputStream.writeInt(data.length);
                    outputStream.write(data);
                } else {
                    outputStream.writeInt(0);
                }
                return channelBuffer;
            } finally {
                Closer.close(outputStream);
            }
        }
    }
}
