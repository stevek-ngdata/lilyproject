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


import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilycms.repository.api.Blob;
import org.lilycms.repository.api.BlobStoreAccess;
import org.lilycms.repository.api.BlobStoreAccessFactory;
import org.lilycms.repository.api.IdGenerator;
import org.lilycms.repository.impl.DFSBlobStoreAccess;
import org.lilycms.repository.impl.HBaseBlobStoreAccess;
import org.lilycms.repository.impl.HBaseRepository;
import org.lilycms.repository.impl.HBaseTypeManager;
import org.lilycms.repository.impl.IdGeneratorImpl;
import org.lilycms.repository.impl.InlineBlobStoreAccess;
import org.lilycms.repository.impl.SizeBasedBlobOutputStreamFactory;
import org.lilycms.testfw.TestHelper;

public class BlobStoreTest {

    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static IdGenerator idGenerator = new IdGeneratorImpl();
    private static HBaseTypeManager typeManager;
    private static HBaseRepository repository;
    private static BlobStoreAccessFactory blobStoreOutputStreamFactory;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);
        typeManager = new HBaseTypeManager(idGenerator, TEST_UTIL.getConfiguration());
        BlobStoreAccess dfsBlobStoreAccess = new DFSBlobStoreAccess(TEST_UTIL.getDFSCluster().getFileSystem());
        BlobStoreAccess hbaseBlobStoreAccess = new HBaseBlobStoreAccess(TEST_UTIL.getConfiguration()); 
        BlobStoreAccess inlineBlobStoreAccess = new InlineBlobStoreAccess(); 
        blobStoreOutputStreamFactory = new SizeBasedBlobOutputStreamFactory(1024, inlineBlobStoreAccess, dfsBlobStoreAccess);
        repository = new HBaseRepository(typeManager, idGenerator, blobStoreOutputStreamFactory, TEST_UTIL.getConfiguration());
        repository.registerBlobStoreAccess(dfsBlobStoreAccess);
        repository.registerBlobStoreAccess(hbaseBlobStoreAccess);
        repository.registerBlobStoreAccess(inlineBlobStoreAccess);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreate() throws Exception {
        byte[] bytes = Bytes.toBytes("someBytes");
        Blob blob = new Blob("aMimetype", (long)bytes.length, "testCreate");
        OutputStream outputStream = repository.getOutputStream(blob);
        outputStream.write(bytes);
        outputStream.close();
        
        InputStream inputStream = repository.getInputStream(blob);
        byte[] readBytes = new byte[blob.getSize().intValue()];
        inputStream.read(readBytes);
        inputStream.close();
        System.out.println(Bytes.toString(readBytes));
        assertTrue(Arrays.equals(bytes, readBytes));
    }
}
