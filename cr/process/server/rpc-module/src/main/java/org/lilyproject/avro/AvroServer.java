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
package org.lilyproject.avro;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.util.concurrent.CustomThreadFactory;
import org.lilyproject.util.concurrent.WaitPolicy;

public class AvroServer {
    private Repository repository;
    private Indexer indexer;
    private int port;
    private int maxServerThreads;
    private ExecutionHandler executionHandler;
    private ExecutorService executorService;

    private Server server;
    private static final int THREAD_POOL_SIZE_RATIO = 3; // the thread pool's max size will be THREAD_RATIO times the pool's core size
    private static final int THREAD_KEEP_ALIVE_SECONDS = 60;

    public AvroServer(Repository repository, Indexer indexer, int port, int maxServerThreads) {
        this.repository = repository;
        this.indexer = indexer;
        this.port = port;
        this.maxServerThreads = maxServerThreads;
    }

    @PostConstruct
    public void start() throws IOException {
        AvroConverter avroConverter = new AvroConverter();
        avroConverter.setRepository(repository);

        AvroLilyImpl avroLily = new AvroLilyImpl(repository, indexer, avroConverter);
        Responder responder = new LilySpecificResponder(AvroLily.class, avroLily, avroConverter);

        ThreadFactory threadFactory = new CustomThreadFactory("avro-exechandler", new ThreadGroup("AvroExecHandler"));
        if (maxServerThreads == -1) {
            executorService = Executors.newCachedThreadPool(threadFactory);
            executionHandler = new ExecutionHandler(executorService);
        } else {
            executorService = new ThreadPoolExecutor(maxServerThreads / THREAD_POOL_SIZE_RATIO, maxServerThreads,
                    THREAD_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), threadFactory, new WaitPolicy());
            executionHandler = new ExecutionHandler(executorService);
        }

        //server = new HttpServer(responder, port);
        server = new NettyServer(responder, new InetSocketAddress(port), new NioServerSocketChannelFactory
                (Executors.newCachedThreadPool(), Executors.newCachedThreadPool()), executionHandler);
        server.start();
    }

    @PreDestroy
    public void stop() {
        // Previously the server.close call was disable for the following reason, just leaving this comment here for
        // historical reasons:
        //    It would be nice to wait for client threads to end, but since these client threads pass into
        //    HBase client code which is notoriously difficult to interrupt, we skip this step
        server.close();

        // Actual work is now being performed on the threads of the ExecutorService
        if (executorService != null) {
            // Interrupt the threads. It would be nicer to wait for them to complete, but I experienced endless
            // hangs on shutdown, to be investigated later.
            executorService.shutdownNow();
            // executionHandler.releaseExternalResources();
        }

        try {
            server.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int getPort() {
        return server.getPort();
    }
}
