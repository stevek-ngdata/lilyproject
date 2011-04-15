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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.lilyproject.rowlog.api.*;

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

    public RemoteListenerHandler(RowLog rowLog, String subscriptionId, RowLogMessageListener rowLogMessageListener,
            RowLogConfigurationManager rowLogConfMgr) throws RowLogException {
        this.rowLog = rowLog;
        this.subscriptionId = subscriptionId;
        this.rowLogMessageListener = rowLogMessageListener;
        this.rowLogConfMgr = rowLogConfMgr;
        bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
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
        InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new RowLogException("Failed to start remote listener", e);
        }
        String hostName = inetAddress.getHostName();
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
    
    private static class MessageDecoder extends FrameDecoder {
        private int msgLength = -1;
        @Override
        protected ChannelBuffer decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            if (msgLength == -1) {
                // Read the message length bytes
                if (buffer.readableBytes() < 4) {
                    return null; 
                } else {
                    ChannelBufferInputStream channelBufferInputStream = new ChannelBufferInputStream(buffer.readBytes(4));
                    msgLength = channelBufferInputStream.readInt();
                    channelBufferInputStream.close();
                    return null;
                }
            } else {
                // Read the message bytes
                if (buffer.readableBytes() < msgLength) {
                    return null;
                } else {
                    ChannelBuffer message = buffer.readBytes(msgLength);
                    msgLength = -1; // Read a message length again the next time decode is called
                    return message; // Give the message to the RowLogMessageDecoder
                }
            }
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
                channel.write(new Boolean(result)).await();
            } else {
                log.warn("Failed to send processing result '"+result+"' for message '"+message+"' due to closed channel.");
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
