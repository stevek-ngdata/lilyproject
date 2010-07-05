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
package org.lilycms.repository.impl.test;


import java.net.InetSocketAddress;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.avro.AvroConverter;
import org.lilycms.repository.avro.AvroRepository;
import org.lilycms.repository.avro.AvroRepositoryImpl;
import org.lilycms.repository.avro.AvroTypeManager;
import org.lilycms.repository.avro.AvroTypeManagerImpl;
import org.lilycms.repository.impl.DFSBlobStoreAccess;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.RepositoryRemoteImpl;
import org.lilycms.repository.impl.SizeBasedBlobStoreAccessFactory;
import org.lilycms.repository.impl.TypeManagerRemoteImpl;
import org.lilycms.testfw.HBaseProxy;
import org.lilycms.testfw.TestHelper;

public class AvroRepositoryTest extends AbstractRepositoryTest {
    private static HBaseRepository serverRepository;

    private final static HBaseProxy HBASE_PROXY = new HBaseProxy();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        HBASE_PROXY.start();
        IdGeneratorImpl idGenerator = new IdGeneratorImpl();
        TypeManager serverTypeManager = new HBaseTypeManager(idGenerator, HBASE_PROXY.getConf());
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(HBASE_PROXY.getBlobFS());
        BlobStoreAccessFactory blobStoreOutputStreamFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        serverRepository = new HBaseRepository(serverTypeManager, idGenerator, blobStoreOutputStreamFactory , HBASE_PROXY.getConf());
        
        AvroConverter serverConverter = new AvroConverter();
        serverConverter.setRepository(serverRepository);
        HttpServer repositoryServer = new HttpServer(
                new SpecificResponder(AvroRepository.class, new AvroRepositoryImpl(serverRepository, serverConverter)),
                0);
        HttpServer typeManagerServer = new HttpServer(
                new SpecificResponder(AvroTypeManager.class, new AvroTypeManagerImpl(serverTypeManager, serverConverter)),
                0);
        AvroConverter remoteConverter = new AvroConverter();
        typeManager = new TypeManagerRemoteImpl(new InetSocketAddress(typeManagerServer.getPort()),
                remoteConverter, idGenerator);
        repository = new RepositoryRemoteImpl(new InetSocketAddress(repositoryServer.getPort()), remoteConverter,
                (TypeManagerRemoteImpl)typeManager, idGenerator);
        remoteConverter.setRepository(repository);
        setupTypes();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        serverRepository.stop();
        HBASE_PROXY.stop();
    }

    @Test
    public void testCreateVariant() throws Exception {
        // TODO Avro side not implemented yet
    }
    
    @Test
    public void testIdRecord() throws Exception {
        // TODO Avro side not implemented yet
    }
}
