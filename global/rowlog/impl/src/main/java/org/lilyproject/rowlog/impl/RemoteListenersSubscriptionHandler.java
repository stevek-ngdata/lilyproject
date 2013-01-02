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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.lilyproject.rowlog.api.RemoteListenerIOException;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.util.concurrent.CustomThreadFactory;
import org.lilyproject.util.io.Closer;

public class RemoteListenersSubscriptionHandler extends AbstractListenersSubscriptionHandler {
    private Log log = LogFactory.getLog(getClass());
    private ClientBootstrap bootstrap;
    private NioClientSocketChannelFactory channelFactory;
    private Map<Integer, RemoteWorkerDelegate> workerDelegates = new ConcurrentHashMap<Integer, RemoteWorkerDelegate>();

    public RemoteListenersSubscriptionHandler(String subscriptionId, MessagesWorkQueue messagesWorkQueue,
                                              RowLog rowLog, RowLogConfigurationManager rowLogConfigurationManager) {
        super(subscriptionId, messagesWorkQueue, rowLog, rowLogConfigurationManager);
        initBootstrap();
    }

    @Override
    protected WorkerDelegate createWorkerDelegate(String host) {
        return new RemoteWorkerDelegate(host);
    }

    private class RemoteWorkerDelegate implements WorkerDelegate {
        private Boolean remoteProcessMessageResult = null;
        private Throwable resultHandlerException = null;
        private Semaphore semaphore = new Semaphore(0);
        private Channel channel = null;
        private String host;

        public RemoteWorkerDelegate(String host) {
            this.host = host;
            initBootstrap();
        }

        private Channel getListenerChannel(String host) throws RemoteListenerIOException, InterruptedException {
            return getListenerChannel(host, 9);
        }

        private Channel getListenerChannel(String host, int triesRemaining) throws RemoteListenerIOException, InterruptedException {
            String listenerHostAndPort[] = host.split(":");
            ChannelFuture connectFuture = bootstrap.connect(new InetSocketAddress(listenerHostAndPort[0],
                    Integer.valueOf(listenerHostAndPort[1])));
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

        /**
         * Processes a message by sending the message to a remote listener.
         * This method retries (5 times) until a communication channel has been successfully setup and a result has been received
         * from the remote listener.
         */
        @Override
        public boolean processMessage(RowLogMessage message) throws RowLogException, InterruptedException {
            return processMessage(message, 4);
        }

        public boolean processMessage(RowLogMessage message, int triesRemaining) throws RowLogException,
                InterruptedException {

            remoteProcessMessageResult = null;
            resultHandlerException = null;
            try {
                if (channel == null || (!channel.isConnected())) {
                    channel = getListenerChannel(host);
                }

                // We can associate 'this' directly with the channel, as we never handle more than one
                // message at a time over a channel. If this ever changes, we should rather generate a per-request ID
                workerDelegates.put(channel.getId(), this);

                ChannelFuture writeFuture = channel.write(message);
                writeFuture.await();
                semaphore.acquire();
                if (remoteProcessMessageResult == null || resultHandlerException != null) {
                    if (triesRemaining > 0) {
                        // Retry
                        if (log.isInfoEnabled()) {
                            log.info("Failed to process message. Retries remaining : " + triesRemaining,
                                    resultHandlerException);
                        }
                        Thread.sleep(10);
                        return processMessage(message, triesRemaining - 1);
                    } else {
                        throw new RemoteListenerIOException("Failure in sending message '" + message +
                                "' to remote listener on host '" + host + "'", resultHandlerException);
                    }
                }

                return remoteProcessMessageResult;
            } catch (InterruptedException e) {
                if (channel != null) {
                    channel.close().awaitUninterruptibly();
                }
                throw e;
            } finally {
                if (channel != null) {
                    workerDelegates.remove(channel.getId());
                }
            }
        }

        @Override
        public void close() {
            if (channel != null) {
                channel.close().awaitUninterruptibly();
            }
        }

    }

    private RemoteWorkerDelegate getWorkerDelegate(ChannelHandlerContext ctx) {
        return workerDelegates.get(ctx.getChannel().getId());
    }


    private void initBootstrap() {
        if (bootstrap == null) {
            if (channelFactory == null) {
                channelFactory = new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(
                                new CustomThreadFactory("rowlog-client-" + rowLog.getId() + "-boss",
                                        new ThreadGroup("RowLogSendToListenerBoss_" + subscriptionId))),
                        Executors.newCachedThreadPool(
                                new CustomThreadFactory("rowlog-client-" + rowLog.getId() + "-worker",
                                        new ThreadGroup("RowLogSendToListener_" + subscriptionId))));
            }
            bootstrap = new ClientBootstrap(channelFactory);
            bootstrap.setPipelineFactory(new ChannelPipelineFactoryImplementation());
            bootstrap.setOption("tcpNoDelay", true);
            bootstrap.setOption("keepAlive", true);
        }
    }

    private final class ChannelPipelineFactoryImplementation implements ChannelPipelineFactory {
        private final ResultDecoder RESULT_DECODER = new ResultDecoder();
        private final MessageEncoder MESSAGE_ENCODER = new MessageEncoder();

        @Override
        public ChannelPipeline getPipeline() {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("resultDecoder", RESULT_DECODER); // Read enough bytes and decode the result
            pipeline.addLast("resultHandler", new ResultHandler()); // Handle the result
            pipeline.addLast("messageEncoder", MESSAGE_ENCODER); // Encode and send the RowLogMessage
            return pipeline;
        }
    }

    private class ResultHandler extends SimpleChannelUpstreamHandler {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            RemoteWorkerDelegate worker = getWorkerDelegate(ctx);
            worker.remoteProcessMessageResult = (Boolean)e.getMessage();
            worker.semaphore.release(); // We received the message, the processMessage call can continue
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            log.debug("Receive response from listener: error occurred", e.getCause());
            RemoteWorkerDelegate worker = getWorkerDelegate(ctx);
            if (worker != null) {
                worker.resultHandlerException = e.getCause();
                worker.semaphore.release(); // An exception occurred, the processMessage call should handle it
            }
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            log.debug("Receive response from listener: channel was closed");
            RemoteWorkerDelegate worker = getWorkerDelegate(ctx);
            if (worker != null) {
                worker.semaphore.release(); // The remoteProcessMessageResult will still be null
                super.channelClosed(ctx, e);
            }
        }
    }

    private class ResultDecoder extends FrameDecoder {
        @Override
        protected Boolean decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            if (buffer.readableBytes() < Bytes.SIZEOF_BOOLEAN) {
                return null;
            }
            return Bytes.toBoolean(buffer.readBytes(Bytes.SIZEOF_BOOLEAN).array()); // Send the result to the ResultHandler
        }
    }

    private class MessageEncoder extends SimpleChannelDownstreamHandler {
        @Override
        public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            ChannelBufferOutputStream outputStream = null;
            try {
                RowLogMessage message = (RowLogMessage)e.getMessage();
                byte[] rowKey = message.getRowKey();
                byte[] data = message.getData();
                int msgLength = 8 + 4 + rowKey.length + 8 + 4; // timestamp + rowkey-length + rowkey + seqnr + data-length + data
                if (data != null) {
                    msgLength = msgLength + data.length;
                }
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
