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
package org.lilyproject.server.modules.repository;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.avro.*;
import org.lilyproject.util.concurrent.CustomThreadFactory;
import org.lilyproject.util.concurrent.WaitPolicy;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

public class AvroServer {
    private String bindAddress;
    private Repository repository;
    private int port;
    private int maxServerThreads;
    private ExecutionHandler executionHandler;
    private ExecutorService executorService;

    private Server server;

    public AvroServer(String bindAddress, Repository repository, int port, int maxServerThreads) {
        this.bindAddress = bindAddress;
        this.repository = repository;
        this.port = port;
        this.maxServerThreads = maxServerThreads;
    }

    @PostConstruct
    public void start() throws IOException {
        AvroConverter avroConverter = new AvroConverter();
        avroConverter.setRepository(repository);

        AvroLilyImpl avroLily = new AvroLilyImpl(repository, avroConverter);
        Responder responder = new LilySpecificResponder(AvroLily.class, avroLily, avroConverter);

        ThreadFactory threadFactory = new CustomThreadFactory("avro-exechandler", new ThreadGroup("AvroExecHandler"));
        if (maxServerThreads == -1) {
            executorService = Executors.newCachedThreadPool(threadFactory);
            executionHandler = new ExecutionHandler(executorService);
        } else {
            executorService = new ThreadPoolExecutor(maxServerThreads / 3, maxServerThreads,
                    60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), threadFactory, new WaitPolicy());
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
            Thread.interrupted();
        }
    }

    public int getPort() {
        return server.getPort();
    }
}
