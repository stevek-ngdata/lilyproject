package org.lilyproject.indexer.engine.test;

import com.google.common.collect.Lists;
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

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class RecordMatcherTest {
    private final static RepositorySetup repoSetup = new RepositorySetup();
    private static Repository repository;
    private static TypeManager typeManager;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        repoSetup.setupCore();
        repoSetup.setupRepository(true);

        repository = repoSetup.getRepository();
        typeManager = repository.getTypeManager();

        FieldType ft1 = typeManager.createFieldType("STRING", new QName("ns", "field1"), Scope.NON_VERSIONED);

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns1")
                .name("typeA")
                .field(ft1.getId(), false)
                .create();

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns1")
                .name("typeB")
                .field(ft1.getId(), false)
                .create();

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns2")
                .name("typeA")
                .field(ft1.getId(), false)
                .create();

        typeManager.recordTypeBuilder()
                .defaultNamespace("ns2")
                .name("typeB")
                .field(ft1.getId(), false)
                .create();

        FieldType case1 = typeManager.createFieldType("LONG", new QName("org.lilyproject.vtag", "case1"),
                Scope.NON_VERSIONED);

        FieldType case2 = typeManager.createFieldType("LONG", new QName("org.lilyproject.vtag", "case2"),
                Scope.NON_VERSIONED);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        repoSetup.stop();
    }

    @Test
    public void testRecordTypeNameWildcard() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList("recordType='ns1:*' vtags='case1'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record recordNs1TypeA = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "field1"), "value1")
                .build();

        Record recordNs2TypeA = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns2", "typeA"))
                .field(new QName("ns", "field1"), "value1")
                .build();

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeA));
        assertNull(idxConf.getRecordFilter().getIndexCase(recordNs2TypeA));
    }

    @Test
    public void testRecordTypeNamespaceWildcard() throws Exception {
        String conf = makeIndexerConf(
                "xmlns:ns1='ns1'",
                Lists.newArrayList(
                        "recordType='*:typeB' vtags='case1'",
                        "recordType='{*}typeC' vtags='case2'"),
                Collections.<String>emptyList()
        );

        IndexerConf idxConf = IndexerConfBuilder.build(new ByteArrayInputStream(conf.getBytes()), repository);

        Record recordNs1TypeB = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeB"))
                .field(new QName("ns", "field1"), "value1")
                .build();

        Record recordNs2TypeB = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns2", "typeB"))
                .field(new QName("ns", "field1"), "value1")
                .build();

        Record recordNs1TypeC = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeC"))
                .field(new QName("ns", "field1"), "value1")
                .build();

        Record recordNs2TypeC = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns2", "typeC"))
                .field(new QName("ns", "field1"), "value1")
                .build();

        Record recordNs1TypeA = repository.recordBuilder()
                .id("record")
                .recordType(new QName("ns1", "typeA"))
                .field(new QName("ns", "field1"), "value1")
                .build();

        assertNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeA));

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeB));
        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs2TypeB));

        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs1TypeC));
        assertNotNull(idxConf.getRecordFilter().getIndexCase(recordNs2TypeC));
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
