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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.lilyproject.rowlog.api.RowLog;
import org.lilyproject.rowlog.api.RowLogConfigurationManager;
import org.lilyproject.rowlog.api.RowLogException;
import org.lilyproject.rowlog.api.RowLogMessage;
import org.lilyproject.rowlog.api.RowLogMessageListener;
import org.lilyproject.util.concurrent.CustomThreadFactory;

public class RemoteListenerHandler {
    private final Log log = LogFactory.getLog(getClass());
    private final RowLogMessageListener rowLogMessageListener;
    private ServerBootstrap bootstrap;
    private final RowLog rowLog;
    private Channel channel;
    private ChannelGroup allChannels = new DefaultChannelGroup("RemoteListenerHandler");
    private String listenerId;
    private final String subscriptionId;
    private final RowLogConfigurationManager rowLogConfMgr;
    private final String hostName;

    public RemoteListenerHandler(RowLog rowLog, String subscriptionId, RowLogMessageListener rowLogMessageListener,
                                 RowLogConfigurationManager rowLogConfMgr, String hostName) throws RowLogException {
        this.rowLog = rowLog;
        this.subscriptionId = subscriptionId;
        this.rowLogMessageListener = rowLogMessageListener;
        this.rowLogConfMgr = rowLogConfMgr;
        this.hostName = hostName;
        bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(
                                new CustomThreadFactory("rowlog-server-" + rowLog.getId() + "-boss",
                                        new ThreadGroup("RowLogListenerBoss_" + subscriptionId))),
                        Executors.newCachedThreadPool(
                                new CustomThreadFactory("rowlog-server-" + rowLog.getId() + "-worker",
                                        new ThreadGroup("RowLogListener_" + subscriptionId)))));
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("messageDecoder", new MessageDecoder()); // Read enough bytes
                pipeline.addLast("rowLogMessageDecoder", new RowLogMessageDecoder()); // Decode the bytes into a RowLogMessage
                pipeline.addLast("messageHandler", new MessageHandler()); // Handle the RowLogMessage
                pipeline.addLast("resultEncoder", new ResultEncoder()); // Encode the result
                return pipeline;
            }
        });

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
    }

    public void start() throws RowLogException, InterruptedException, KeeperException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostName, 0);
        channel = bootstrap.bind(inetSocketAddress);
        allChannels.add(channel);
        int port = ((InetSocketAddress)channel.getLocalAddress()).getPort();
        listenerId = hostName + ":" + port;
        rowLogConfMgr.addListener(rowLog.getId(), subscriptionId, listenerId);
    }

    public void stop() throws InterruptedException {
        ChannelGroupFuture future = allChannels.close();
        future.awaitUninterruptibly();

        bootstrap.releaseExternalResources();

        if (listenerId != null) {
            try {
                rowLogConfMgr.removeListener(rowLog.getId(), subscriptionId, listenerId);
            } catch (KeeperException e) {
                log.warn("Exception while removing listener. Row log ID " + rowLog.getId() + ", subscription ID " + subscriptionId +
                        ", listener ID " + listenerId, e);
            } catch (RowLogException e) {
                log.warn("Exception while removing listener. Row log ID " + rowLog.getId() + ", subscription ID " + subscriptionId +
                        ", listener ID " + listenerId, e);
            }
        }
    }

    private class MessageDecoder extends FrameDecoder {
        @Override
        protected ChannelBuffer decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            // Make sure if the length field was received.
            if (buffer.readableBytes() < 4) {
                // The length field was not received yet - return null.
                // This method will be invoked again when more packets are
                // received and appended to the buffer.
                return null;
            }

            // The length field is in the buffer.

            // Mark the current buffer position before reading the length field
            // because the whole frame might not be in the buffer yet.
            // We will reset the buffer position to the marked position if
            // there's not enough bytes in the buffer.
            buffer.markReaderIndex();

            // Read the length field.
            int length = buffer.readInt();

            // Make sure if there's enough bytes in the buffer.
            if (buffer.readableBytes() < length) {
                // The whole bytes were not received yet - return null.
                // This method will be invoked again when more packets are
                // received and appended to the buffer.

                // Reset to the marked position to read the length field again
                // next time.
                buffer.resetReaderIndex();

                return null;
            }

            // There's enough bytes in the buffer. Read it.
            ChannelBuffer frame = buffer.readBytes(length);

            // Successfully decoded a frame.  Return the decoded frame.
            return frame;
        }
    }

    private class RowLogMessageDecoder extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            ChannelBufferInputStream inputStream = new ChannelBufferInputStream((ChannelBuffer)e.getMessage());

            long timestamp = inputStream.readLong();

            int rowKeyLength = inputStream.readInt();
            byte[] rowKey = new byte[rowKeyLength];
            inputStream.readFully(rowKey, 0, rowKeyLength);

            long seqnr = inputStream.readLong();

            int dataLength = inputStream.readInt();
            byte[] data = null;
            if (dataLength > 0) {
                data = new byte[dataLength];
                inputStream.readFully(data, 0, dataLength);
            }
            inputStream.close();
            RowLogMessage rowLogMessage = new RowLogMessageImpl(timestamp, rowKey, seqnr, data, rowLog);
            Channels.fireMessageReceived(ctx, rowLogMessage); // Give the message to the MessageHandler
        }
    }

    private class MessageHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            RowLogMessage message = (RowLogMessage)e.getMessage();
            boolean result = rowLogMessageListener.processMessage(message);
            writeResult(e.getChannel(), result, message);
        }

        private void writeResult(Channel channel, boolean result, RowLogMessage message) throws InterruptedException {
            if (channel.isOpen()) {
                channel.write(Boolean.valueOf(result)).await();
            } else {
                log.warn("Failed to send processing result '" + result + "' for message '" + message + "' due to closed channel.");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            log.warn("Exception in MessageHandler while processing message", e.getCause());
            // We won't retry sending the result to avoid exception-loops
            // Instead, close the channel so that the client channel gets closed as well.
            // The client (RemoteListenersSubscriptionHandler) will then retry to set up the channel and send the message 
            e.getChannel().close();
        }

        @Override
        public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
            allChannels.add(e.getChannel()); // Put the channel in the channel group so that it will be closed upon shutdown
            super.channelOpen(ctx, e);
        }
    }

    private class ResultEncoder extends SimpleChannelDownstreamHandler {
        @Override
        public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Boolean result = (Boolean)e.getMessage();
            ChannelBuffer channelBuffer = ChannelBuffers.buffer(Bytes.SIZEOF_BOOLEAN);
            channelBuffer.writeBytes(Bytes.toBytes(result));
            Channels.write(ctx, e.getFuture(), channelBuffer);
        }
    }
}
