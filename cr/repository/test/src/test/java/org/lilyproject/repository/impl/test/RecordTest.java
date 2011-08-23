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
package org.lilyproject.repository.impl.test;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.*;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.hadooptestfw.TestHelper;
import org.lilyproject.repository.api.*;
import org.lilyproject.repository.impl.primitivevaluetype.AbstractValueType;
import org.lilyproject.repotestfw.RepositorySetup;

public class RecordTest {

    private static final RepositorySetup repoSetup = new RepositorySetup();

    private static TypeManager typeManager;
    private static Repository repository;
    private static IdGenerator idGenerator;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        // TODO this test relies on all blobs being inline blobs, since it reuses blob values
        repoSetup.setBlobLimits(Long.MAX_VALUE, -1);
        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        typeManager = repoSetup.getTypeManager();
        repository = repoSetup.getRepository();
        idGenerator = repoSetup.getIdGenerator();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
  @Test
  public void testCloneRecord() throws Exception {
      String namespace = "testCloneRecord";
      QName stringFieldName = new QName(namespace, "stringField");
      QName integerFieldName = new QName(namespace, "integerField");
      QName longFieldName = new QName(namespace, "longField");
      QName doubleFieldName = new QName(namespace, "doubleField");
      QName decimalFieldName = new QName(namespace, "decimalField");
      QName booleanFieldName = new QName(namespace, "booleanField");
      QName dateFieldName = new QName(namespace, "dateField");
      QName dateTimeFieldName = new QName(namespace, "dateTimeField");
      QName linkFieldName = new QName(namespace, "linkField");
      QName blobFieldName = new QName(namespace, "blobField");
      QName uriFieldName = new QName(namespace, "uriField");
      QName listFieldName = new QName(namespace, "listField");
      QName pathFieldName = new QName(namespace, "pathField");
      QName recordFieldName = new QName(namespace, "recordField");
      QName recordFieldName2 = new QName(namespace, "recordField2");

      
      String stringValue = "abc";
      Integer integerValue = 123;
      Long longValue = 123L;
      Double doubleValue = new Double(2.2d);
      BigDecimal decimalValue = BigDecimal.valueOf(Double.MIN_EXPONENT);
      Boolean booleanValue = true;
      LocalDate dateValue = new LocalDate(2900, 10, 14);
      DateTime dateTimeValue = new DateTime(Long.MAX_VALUE);
      Link linkValue = new Link(idGenerator.newRecordId());
      Blob blobValue = new Blob(Bytes.toBytes("anotherKey"), "image/jpeg", Long.MIN_VALUE, "images/image.jpg");
      URI uriValue = URI.create("http://foo.com/bar");
      List<String> listValue = new ArrayList<String>();
      listValue.add("abc");
      HierarchyPath pathValue = new HierarchyPath("abc");
      Record recordValue = repository.recordBuilder().field(stringFieldName, "foo").newRecord();
      
      Record record = repository.recordBuilder()
          .field(stringFieldName, stringValue)
          .field(integerFieldName, integerValue)
          .field(longFieldName, longValue)
          .field(doubleFieldName, doubleValue)
          .field(decimalFieldName, decimalValue)
          .field(booleanFieldName, booleanValue)
          .field(dateFieldName, dateValue)
          .field(dateTimeFieldName, dateTimeValue)
          .field(linkFieldName, linkValue)
          .field(blobFieldName, blobValue)
          .field(uriFieldName, uriValue)
          .field(listFieldName, listValue)
          .field(pathFieldName, pathValue)
          .field(recordFieldName, recordValue)
          .newRecord();
      
      // Put a record in itself
      // This should normally not be done, but we need to test that the clone method does not choke on this.
      record.setField(recordFieldName2, record);
      
      // Clone record
      record = record.clone();
      
      // Change mutable values
      listValue.add("def");
      pathValue.getElements()[0] = "def";
      blobValue.setSize(0L);
      recordValue.setField(integerFieldName, 777);
               
      // Validate cloned record does not contain mutations
      List<String> list = record.getField(listFieldName);
      assertTrue(list.size() == 1);
      assertEquals("abc", list.get(0));
      
      HierarchyPath path = record.getField(pathFieldName);
      assertEquals("abc", path.getElements()[0]);
      
      Blob blob = record.getField(blobFieldName);
      assertTrue(Long.MIN_VALUE == blob.getSize());
      
      Record recordField = record.getField(recordFieldName);
      assertFalse(recordField.hasField(integerFieldName));
      
      assertTrue(record == record.getField(recordFieldName2));
  }
    
}
