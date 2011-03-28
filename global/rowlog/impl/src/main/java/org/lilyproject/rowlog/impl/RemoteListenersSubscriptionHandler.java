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
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RemoteListenerIOException;
import org.lilyproject.util.io.Closer;

public class RemoteListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    private ClientBootstrap bootstrap;
    private NioClientSocketChannelFactory channelFactory;
    private Boolean remoteProcessMessageResult = null;
    private Throwable resultHandlerException = null;
    private Semaphore semaphore = new Semaphore(0);
    private Log log = LogFactory.getLog(getClass());
    private Channel channel;

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
    protected boolean processMessage(String host, RowLogMessage message) throws RemoteListenerIOException, InterruptedException {
        return processMessage(host, message, 4);
    }
    
    
    private boolean processMessage(String host, RowLogMessage message, int triesRemaining) throws RemoteListenerIOException, InterruptedException {
        remoteProcessMessageResult = null;
        resultHandlerException = null;
        channel = null;
        try {
            if (channel == null || (!channel.isConnected())) {
                channel = getListenerChannel(host);
            }
    
            ChannelFuture writeFuture = channel.write(message);
            writeFuture.await();
            semaphore.acquire();
            if (remoteProcessMessageResult == null || resultHandlerException != null) {
                if (triesRemaining > 0) {
                    // Retry
                    log.info("Failed to process message. Retries remaining : " + triesRemaining, resultHandlerException);
                    Thread.sleep(10);
                    return processMessage(host, message, triesRemaining-1);
                } else {
                    throw new RemoteListenerIOException("Failure in sending message '"+message+"' to remote listener on host '" +host+"'", resultHandlerException);
                }
            }
            return remoteProcessMessageResult;
        } catch (InterruptedException e) {
            if (channel != null) {
                channel.close().awaitUninterruptibly();
            }
            throw e;
        }
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
    
    @Override
    public void shutdown() {
        super.shutdown(); // This will stop all workers first
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        semaphore.release(); // We can safely release the semaphore, no worker is waiting on the result of processMessage anymore
    }
    
    private Channel getListenerChannel(String host) throws RemoteListenerIOException, InterruptedException {
        return getListenerChannel(host, 9);
    }
    
    private Channel getListenerChannel(String host, int triesRemaining) throws RemoteListenerIOException, InterruptedException {
        String listenerHostAndPort[] = host.split(":");
        ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(listenerHostAndPort[0], Integer
                .valueOf(listenerHostAndPort[1])));
        connectFuture.await();
        if (connectFuture.isSuccess()) {
            return connectFuture.getChannel();
        } else {
            if (triesRemaining > 0) {
                Thread.sleep(10);
                return getListenerChannel(host, triesRemaining - 1);
            } else {
                throw new RemoteListenerIOException("Failed to connect channel to remote listener on host '" + host + "'");
            }
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
            pipeline.addLast("resultDecoder", RESULT_DECODER); // Read enough bytes and decode the result
            pipeline.addLast("resultHandler", remoteListenersSubscriptionHandler.new ResultHandler()); // Handle the result
            pipeline.addLast("messageEncoder", MESSAGE_ENCODER); // Encode and send the RowLogMessage
            return pipeline;
        }
    }

    private static class ResultDecoder extends FrameDecoder {
        @Override
        protected Boolean decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            if (buffer.readableBytes() < Bytes.SIZEOF_BOOLEAN) {
                return null;
            }
            return Bytes.toBoolean(buffer.readBytes(Bytes.SIZEOF_BOOLEAN).array()); // Send the result to the ResultHandler
        }
    }

    private class ResultHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            remoteProcessMessageResult = (Boolean) e.getMessage();
            semaphore.release(); // We received the message, the processMessage call can continue
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            resultHandlerException = e.getCause();
            semaphore.release(); // An exception occured, the processMessagge call should handle it
        }
        
        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            semaphore.release(); // The remoteProcessMessageResult will still be null
            super.channelClosed(ctx, e);
        }
    }

    private static class MessageEncoder extends SimpleChannelDownstreamHandler {
        @Override
        public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            ChannelBufferOutputStream outputStream = null;
            try {
                RowLogMessage message = (RowLogMessage) e.getMessage();
                byte[] rowKey = message.getRowKey();
                byte[] data = message.getData();
                int msgLength = 8 + 4 + rowKey.length + 8 + 4; // timestamp + rowkey-length + rowkey + seqnr + data-length + data
                if (data != null)
                    msgLength = msgLength + data.length;
                ChannelBuffer channelBuffer = ChannelBuffers.buffer(4 + msgLength);
                outputStream = new ChannelBufferOutputStream(channelBuffer);
                outputStream.writeInt(msgLength);
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
                Channels.write(ctx, e.getFuture(), channelBuffer);
            } finally {
                Closer.close(outputStream);
            }
        }
    }
}
