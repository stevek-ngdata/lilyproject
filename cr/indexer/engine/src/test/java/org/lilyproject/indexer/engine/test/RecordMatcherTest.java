/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.indexer.engine.test;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repotestfw.RepositorySetup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class RecordMatcherTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();
    private static Repository repository;
    private static TypeManager typeManager;

    private static FieldType vtag1;
    private static FieldType vtag2;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository();

        repository = repoSetup.getRepository();
        typeManager = repository.getTypeManager();

        FieldType stringField = typeManager.createFieldType("STRING", new QName("ns", "string"), Scope.NON_VERSIONED);
        FieldType booleanField = typeManager.createFieldType("BOOLEAN", new QName("ns", "bool"), Scope.NON_VERSIONED);
        FieldType intField = typeManager.createFieldType("INTEGER", new QName("ns", "int"), Scope.NON_VERSIONED);

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns1")
                .name("typeA")
                .field(stringField.getId(), false)
                .create();

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns1")
                .name("typeB")
                .field(stringField.getId(), false)
                .create();

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns2")
                .name("typeA")
                .field(stringField.getId(), false)
                .create();

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns2")
                .name("typeB")
                .field(stringField.getId(), false)
                .create();

        vtag1 = typeManager.createFieldType("LONG", new QName("org.lilyproject.vtag", "vtag1"),
                Scope.NON_VERSIONED);

        vtag2 = typeManager.createFieldType("LONG", new QName("org.lilyproject.vtag", "vtag2"),
                Scope.NON_VERSIONED);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();
    }

    @Test
    public void testRecordType() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList(
                        "recordType='ns1:typeA' vtags='vtag1'",
                        "recordType='{ns1}typeB' vtags='vtag2'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record recordTypeA = newRecordOfType(new QName("ns1", "typeA"));
        Record recordTypeB = newRecordOfType(new QName("ns1", "typeB"));

        assertEquals(ImmutableSet.of(vtag1.getId()),
                idxConf.getRecordFilter().getIndexCase(recordTypeA).getVersionTags());
        assertEquals(ImmutableSet.of(vtag2.getId()),
                idxConf.getRecordFilter().getIndexCase(recordTypeB).getVersionTags());
    }

    @Test
    public void testRecordTypeNameWildcard() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList("recordType='ns1:*' vtags='vtag1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record recordNs1TypeA = newRecordOfType(new QName("ns1", "typeA"));
        Record recordNs2TypeA = newRecordOfType(new QName("ns2", "typeA"));

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeA));
        assertNull(idxConf.getRecordFilter().getIndexCase(recordNs2TypeA));
    }

    @Test
    public void testRecordTypeNamespaceWildcard() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList(
                        "recordType='*:typeB' vtags='vtag1'",
                        "recordType='{*}typeC' vtags='vtag2'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record recordNs1TypeB = newRecordOfType(new QName("ns1", "typeB"));
        Record recordNs2TypeB = newRecordOfType(new QName("ns2", "typeB"));
        Record recordNs1TypeC = newRecordOfType(new QName("ns1", "typeC"));
        Record recordNs2TypeC = newRecordOfType(new QName("ns2", "typeC"));
        Record recordNs1TypeA = newRecordOfType(new QName("ns1", "typeA"));

        assertNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeA));

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeB));
        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs2TypeB));

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeC));
        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs2TypeC));
    }

    @Test
    public void testNullRecordTypeDoesNotMatchSpecifiedRecordType() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList(
                        "recordType='ns1:typeA' vtags='vtag1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record record = repository.recordBuilder().id("record").build();

        assertNull(idxConf.getRecordFilter().getIndexCase(record));
    }

    @Test
    public void testNullRecordTypeMatchesAbsentRecordTypeCondition() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList(
                        "vtags='vtag1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record record = repository.recordBuilder().id("record").build();

        assertNotNull(idxConf.getRecordFilter().getIndexCase(record));
    }

    @Test
    public void testEqualsFieldCondition() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList(
                        "field='ns:string=zeus' vtags='vtag1'",
                        "field='ns:bool=true' vtags='vtag2'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        //
        // Test string field
        //
        Record zeus = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "zeus")
                .field(new QName("ns", "bool"), Boolean.TRUE)
                .field(new QName("ns", "int"), 5)
                .build();

        Record hera = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "hera")
                .field(new QName("ns", "int"), 10)
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(hera));
        assertEquals(ImmutableSet.of(vtag1.getId()), idxConf.getRecordFilter().getIndexCase(zeus).getVersionTags());

        //
        // Test boolean field
        //
        Record trueRecord = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "bool"), Boolean.TRUE)
                .build();

        Record falseRecord = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "bool"), Boolean.FALSE)
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(falseRecord));
        assertEquals(ImmutableSet.of(vtag2.getId()),
                idxConf.getRecordFilter().getIndexCase(trueRecord).getVersionTags());
    }

    @Test
    public void testNotEqualsFieldCondition() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList("field='ns:bool!=true' vtags='vtag1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record trueRecord = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "bool"), Boolean.TRUE)
                .build();

        Record falseRecord = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "bool"), Boolean.FALSE)
                .build();

        Record nullRecord = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(trueRecord));

        // false and null field value are both treated as being not equal to true
        assertNotNull(idxConf.getRecordFilter().getIndexCase(falseRecord));
        assertNotNull(idxConf.getRecordFilter().getIndexCase(nullRecord));
    }

    @Test
    public void testVariantProperties() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList(
                        "variant='prop1,prop2=artemis' vtags='vtag1'",
                        "variant='prop1,prop2,*' vtags='vtag2'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        //
        // Record with exactly two properties should be matched by first rule
        //
        Record recordProp1Prop2 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1", "prop2", "artemis"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "something")
                .build();

        assertEquals(Sets.newHashSet(vtag1.getId()),
                idxConf.getRecordFilter().getIndexCase(recordProp1Prop2).getVersionTags());

        //
        // Record with more properties than prop1 & prop2 should be matched by second rule
        //
        Record recordProp1Prop2Prop3 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1", "prop2", "artemis", "prop3", "val3"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "something")
                .build();

        assertEquals(Sets.newHashSet(vtag2.getId()),
                idxConf.getRecordFilter().getIndexCase(recordProp1Prop2Prop3).getVersionTags());

        //
        // Record with one prop should not be matched by any rules
        //
        Record recordProp1 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "something")
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(recordProp1));
    }

    @Test
    public void testVariantPropertiesMatchNone() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList("variant='' vtags='vtag1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        // An empty variant expression should only match record without variant properties
        Record recordProp1 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "something")
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(recordProp1));

        Record record = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "something")
                .build();

        assertNotNull(idxConf.getRecordFilter().getIndexCase(record));
    }

    @Test
    public void testVariantPropertiesMatchAll() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList("vtags='vtag1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        // There are no conditions on variant properties, so a record id with variant properties should pass
        Record recordProp1 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "string"), "something")
                .build();

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordProp1));
    }

    @Test
    public void testExcludes() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList("recordType='*:typeA' vtags='vtag1'"),
                Lists.newArrayList("recordType='ns2:*'")
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record recordNs1TypeA = newRecordOfType(new QName("ns1", "typeA"));
        Record recordNs2TypeA = newRecordOfType(new QName("ns2", "typeA"));

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeA));
        assertNull(idxConf.getRecordFilter().getIndexCase(recordNs2TypeA));
    }

    @Test
    public void testAllCombined() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList("recordType='ns1:typeA' variant='prop1=val1' field='ns:int=10' vtags='vtag1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record record1 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "int"), new Integer(10))
                .build();

        assertNotNull(idxConf.getRecordFilter().getIndexCase(record1));

        Record record2 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "int"), new Integer(11))
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(record2));

        Record record3 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeB"))
                .field(new QName("ns", "int"), new Integer(10))
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(record3));

        Record record4 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val2"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "int"), new Integer(10))
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(record4));
    }

    @Test
    public void testNoConditionsOnInclude() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList("vtags='vtag1'"), /* an include without conditions */
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        // A record of any type with any number of variant properties should match
        Record record1 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "int"), new Integer(10))
                .build();

        assertNotNull(idxConf.getRecordFilter().getIndexCase(record1));
    }

    @Test
    public void testNoConditionsOnExclude() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1' xmlns:ns='ns'",
                Lists.newArrayList("vtags='vtag1'"),
                Lists.newArrayList("")
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        // A record of any type with any number of variant properties should match
        Record record1 = repository.recordBuilder()
                .id("record", ImmutableMap.of("prop1", "val1"))
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "int"), new Integer(10))
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(record1));
    }

    private Record newRecordOfType(QName recordType) throws Exception {
        return repository.recordBuilder()
                .id("record")
                .recordType(recordType)
                .build();
    }

    private String makeIndexerConf(String namespaces, List<String> includes, List<String> excludes) {
        StringBuilder result = new StringBuilder();
        result.append("<indexer ").append(namespaces).append(">\n<recordFilter>\n");

        if (includes.size() > 0) {
            result.append("<includes>\n");
            for (String include : includes) {
                result.append("<include ").append(include).append("/>\n");
            }
            result.append("</includes>\n");
        }

        if (includes.size() > 0) {
            result.append("<excludes>\n");
            for (String exclude : excludes) {
                result.append("<exclude ").append(exclude).append("/>\n");
            }
            result.append("</excludes>\n");
        }

        result.append("</recordFilter>\n</indexer>\n");

        return result.toString();
    }
}
