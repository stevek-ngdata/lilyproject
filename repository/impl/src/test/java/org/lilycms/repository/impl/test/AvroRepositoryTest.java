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

import org.apache.avro.ipc.SocketServer;
import org.apache.avro.specific.SpecificResponder;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.Repository;
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
import org.lilycms.testfw.TestHelper;

public class AvroRepositoryTest extends AbstractRepositoryTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        IdGeneratorImpl idGenerator = new IdGeneratorImpl();
		TypeManager serverTypeManager = new HBaseTypeManager(idGenerator, TEST_UTIL.getConfiguration());
        DFSBlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(TEST_UTIL.getDFSCluster().getFileSystem());
        BlobStoreAccessFactory blobStoreOutputStreamFactory = new SizeBasedBlobStoreAccessFactory(dfsBlobStoreAccess);
        Repository serverRepository = new HBaseRepository(serverTypeManager, idGenerator, blobStoreOutputStreamFactory , TEST_UTIL.getConfiguration());
		
        AvroConverter serverConverter = new AvroConverter();
        serverConverter.setRepository(serverRepository);
        SocketServer repositorySocketServer = new SocketServer(new SpecificResponder(AvroRepository.class, new AvroRepositoryImpl(serverRepository, serverConverter)),
        		new InetSocketAddress(0)); 
        SocketServer typeManagerSocketServer = new SocketServer(new SpecificResponder(AvroTypeManager.class, new AvroTypeManagerImpl(serverTypeManager, serverConverter)),
				new InetSocketAddress(0));
        AvroConverter remoteConverter = new AvroConverter();
        typeManager = new TypeManagerRemoteImpl(new InetSocketAddress(typeManagerSocketServer.getPort()),
                remoteConverter, idGenerator);
        repository = new RepositoryRemoteImpl(new InetSocketAddress(repositorySocketServer.getPort()), remoteConverter,
                (TypeManagerRemoteImpl)typeManager, idGenerator);
        remoteConverter.setRepository(repository);
        setupTypes();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
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
