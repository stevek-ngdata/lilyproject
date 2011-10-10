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
package org.lilyproject.process.test;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.client.LilyClient;
import org.lilyproject.lilyservertestfw.LilyProxy;
import org.lilyproject.repository.api.*;

public class LilyClientTest {
    private static LilyProxy lilyProxy;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        lilyProxy = new LilyProxy();
        lilyProxy.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            if (lilyProxy != null)
                lilyProxy.stop();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /**
     * Creates a record with a blob using a Repository obtained via LilyClient. This verifies
     * that the blobs work remotely and it was able to retrieve the blob stores config from
     * ZooKeeper.
     */
    @Test
    public void testBlob() throws Exception {
        LilyClient client = lilyProxy.getLilyServerProxy().getClient();

        // Obtain a repository
        Repository repository = client.getRepository();

        // String NS = "org.lilyproject.client.test";
        String NS = null;

        // Create a blob field type and record type
        TypeManager typeManager = repository.getTypeManager();
        ValueType blobType = typeManager.getValueType("BLOB");
        FieldType blobFieldType = typeManager.newFieldType(blobType, new QName(NS, "data"), Scope.VERSIONED);
        blobFieldType = typeManager.createFieldType(blobFieldType);

        RecordType recordType = typeManager.newRecordType(new QName(NS, "file"));
        recordType.addFieldTypeEntry(blobFieldType.getId(), true);
        recordType = typeManager.createRecordType(recordType);


        // Upload a blob that, based upon the current default config, should end up in HBase
        //  (> 5000 bytes and < 200000 bytes)
        byte[] data = makeBlobData(10000);
        Blob blob = new Blob("application/octet-stream", (long)data.length, null);
        OutputStream blobStream = repository.getOutputStream(blob);
        IOUtils.copy(new ByteArrayInputStream(data), blobStream);
        blobStream.close();
        assertTrue(blob.getValue() != null);

        // Create a record wit this blob
        Record record = repository.newRecord();
        record.setRecordType(new QName(NS, "file"));
        record.setField(new QName(NS, "data"), blob);
        record = repository.create(record);
    }

    private byte[] makeBlobData(int size) {
        return new byte[size];
    }

}
