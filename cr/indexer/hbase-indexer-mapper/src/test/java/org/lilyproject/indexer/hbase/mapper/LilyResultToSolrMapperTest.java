package org.lilyproject.indexer.hbase.mapper;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Metadata;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.FieldFlags;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repotestfw.FakeRepositoryManager;
import org.lilyproject.tools.import_.cli.JsonImport;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.repo.RecordEvent;

import java.util.List;

public class LilyResultToSolrMapperTest {
    private static final String NS1 = "org.lilyproject.indexer.hbase.mapper.test";
    private static final String NS2 = "org.lilyproject.indexer.hbase.mapper.test.2";
    
    private LilyResultToSolrMapper mapper;
    private RepositoryManager repositoryManager;
    private LRepository repository;
    private FakeSolrUpdateWriter solrUpdateWriter;
    @Before
    public void setup () throws Exception{
        repositoryManager = FakeRepositoryManager.bootstrapRepositoryManager();
        repository = repositoryManager.getDefaultRepository();
        
        JsonImport.loadSchema(repository, LilyResultToSolrMapperTest.class.getResourceAsStream("schema.json"));

        mapper = new LilyResultToSolrMapper();
        mapper.setRepositoryManager(repositoryManager);
        String indexerConf = IOUtils.toString(LilyResultToSolrMapperTest.class.getResourceAsStream("indexer-conf.xml"));
        mapper.setIndexerConfString(indexerConf);
        mapper.setIndexName("theindex");

        mapper.init();

        solrUpdateWriter = new FakeSolrUpdateWriter();
    }

    @Test
    public void testMap() throws Exception {
        LTable table = repository.getDefaultTable();
        Record record = table.recordBuilder()
                .assignNewUuid()
                .recordType(new QName(NS1, "rt1"))
                .field(new QName(NS1, "a_string"), "myvalue")
                .create();
        
        Result result = encodeRecord(record);
        
        mapper.map(result, solrUpdateWriter);
        Assert.assertFalse(solrUpdateWriter.documents.isEmpty());
        SolrInputDocument doc = solrUpdateWriter.documents.get(0);
        Assert.assertTrue(doc.containsKey("a_string"));
        Assert.assertEquals("myvalue", doc.get("a_string").getValue());
    }


    public Result encodeRecord(Record record) throws InterruptedException, RepositoryException {
        TypeManager typeManager = repositoryManager.getDefaultRepository().getTypeManager();
        List<KeyValue> kvs = Lists.newArrayList();

        RecordEvent event = new RecordEvent();
        event.setType(RecordEvent.Type.CREATE);
        event.setTableName(LilyHBaseSchema.Table.RECORD.name);
        event.setVersionCreated(record.getVersion());

        byte[] rowKey = record.getId().toBytes();
        byte[] family = LilyHBaseSchema.RecordCf.DATA.bytes;

        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.VERSION.bytes, 1L, Bytes.toBytes(record.getVersion())));
        // TODO deleted?

        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.NON_VERSIONED_RT_ID.bytes,
                1L, typeManager.getRecordTypeByName(record.getRecordTypeName(Scope.NON_VERSIONED),
                        record.getRecordTypeVersion(Scope.NON_VERSIONED)).getId().getBytes()));
        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.NON_VERSIONED_RT_VERSION.bytes,
                1L, Bytes.toBytes(record.getRecordTypeVersion(Scope.NON_VERSIONED))));
        /*
        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.VERSIONED_RT_ID.bytes,
                typeManager.getRecordTypeByName(record.getRecordTypeName(Scope.VERSIONED),
                        record.getRecordTypeVersion(Scope.VERSIONED)).getId().getBytes()));
        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.VERSIONED_RT_VERSION.bytes,
                Bytes.toBytes(record.getRecordTypeVersion(Scope.VERSIONED))));

        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.VERSIONED_MUTABLE_RT_ID.bytes,
                typeManager.getRecordTypeByName(record.getRecordTypeName(Scope.VERSIONED_MUTABLE),
                        record.getRecordTypeVersion(Scope.VERSIONED_MUTABLE)).getId().getBytes()));
        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.NON_VERSIONED_RT_VERSION.bytes,
                Bytes.toBytes(record.getRecordTypeVersion(Scope.VERSIONED_MUTABLE))));
                */

        for (QName fieldName : record.getFields().keySet()) {
            FieldType fieldType = typeManager.getFieldTypeByName(fieldName);
            Metadata metadata = record.getMetadata(fieldName);
            boolean hasMetadata = metadata != null && !metadata.getMap().isEmpty();

            DataOutput output = new DataOutputImpl();

            output.writeByte(hasMetadata ? FieldFlags.METADATA_V1 : FieldFlags.DEFAULT);
            output.writeBytes(fieldType.getValueType().toBytes(record.getField(fieldName), new IdentityRecordStack()));
            if (hasMetadata) {
                HBaseRepository.writeMetadataWithLengthSuffix(metadata, output);
            }

            event.addUpdatedField(fieldType.getId());

            kvs.add(new KeyValue(rowKey, family,
                    Bytes.add(new byte[]{LilyHBaseSchema.RecordColumn.DATA_PREFIX}, fieldType.getId().getBytes()),
                    output.toByteArray()));
        }
        
        // fields to delete, should we add these too?
        kvs.add(new KeyValue(rowKey, family, LilyHBaseSchema.RecordColumn.PAYLOAD.bytes, event.toJsonBytes()));

        return new Result(kvs);
    }

    private static class FakeSolrUpdateWriter implements SolrUpdateWriter {
        private List<SolrInputDocument> documents = Lists.newArrayList();
        @Override
        public void add(SolrInputDocument solrDocument) {
            documents.add(solrDocument);
        }

        @Override
        public void deleteById(String documentId) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void deleteByQuery(String query) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
