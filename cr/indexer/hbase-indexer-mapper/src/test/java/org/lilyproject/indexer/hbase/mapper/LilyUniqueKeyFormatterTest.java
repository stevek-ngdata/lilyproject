package org.lilyproject.indexer.hbase.mapper;

import com.google.common.base.Joiner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repotestfw.FakeRepositoryManager;
import org.lilyproject.util.hbase.LilyHBaseSchema;

import java.util.Arrays;

public class LilyUniqueKeyFormatterTest {
    private LilyUniqueKeyFormatter keyFormatter;
    private RepositoryManager repositoryManager;
    private FieldType fieldType;
    @Before
    public void setup() throws Exception{
        repositoryManager = FakeRepositoryManager.bootstrapRepositoryManager();
        TypeManager typeManager = repositoryManager.getDefaultRepository().getTypeManager();
        fieldType = typeManager.createFieldType("STRING", new QName("org.lilyproject.test.ns", "uniq"), Scope.NON_VERSIONED);

        keyFormatter = new LilyUniqueKeyFormatter();
        keyFormatter.setRepositoryManager(repositoryManager);
        keyFormatter.setRepositoryName("default");
        keyFormatter.init();

    }

    @Test
    public void testFormatFamily () {
        Assert.assertEquals(LilyHBaseSchema.RecordCf.DATA.name,
                keyFormatter.formatFamily(LilyHBaseSchema.RecordCf.DATA.bytes));
    }

    @Test
    public void testFormatRow() throws Exception {
        RecordId recordId = repositoryManager.getDefaultRepository().getIdGenerator().newRecordId();
        Assert.assertEquals(recordId.toString(), keyFormatter.formatRow(recordId.toBytes()));
    }

    @Test
    public void testFormatKeyValue() throws Exception{
        RecordId recordId = repositoryManager.getDefaultRepository().getIdGenerator().newRecordId();
        KeyValue kv = new KeyValue(
                recordId.toBytes(),
                LilyHBaseSchema.RecordCf.DATA.bytes,
                Bytes.add(new byte[]{LilyHBaseSchema.RecordColumn.DATA_PREFIX}, fieldType.getId().getBytes()));
        String expect = Joiner.on("|").join(recordId.toString(), LilyHBaseSchema.RecordCf.DATA.name,
                Bytes.toString(new byte[] {LilyHBaseSchema.RecordColumn.DATA_PREFIX} ) + fieldType.getName().toString());
        Assert.assertEquals(expect, keyFormatter.formatKeyValue(kv));
    }

    @Test
    public void testUnformatFamily() {
        String formatted = keyFormatter.formatFamily(LilyHBaseSchema.RecordCf.DATA.bytes);
        Assert.assertTrue(Arrays.equals(LilyHBaseSchema.RecordCf.DATA.bytes, keyFormatter.unformatFamily(formatted)));
    }

    @Test
    public void testUnformatRow() throws Exception{
        RecordId recordId = repositoryManager.getDefaultRepository().getIdGenerator().newRecordId();
        String formatted =  keyFormatter.formatRow(recordId.toBytes());
        Assert.assertTrue(Arrays.equals(recordId.toBytes(), keyFormatter.unformatRow(formatted)));
    }

    @Test
    public void testUnformatKeyValue() throws Exception{
        RecordId recordId = repositoryManager.getDefaultRepository().getIdGenerator().newRecordId();
        KeyValue kv = new KeyValue(
                recordId.toBytes(),
                LilyHBaseSchema.RecordCf.DATA.bytes,
                Bytes.add(new byte[]{LilyHBaseSchema.RecordColumn.DATA_PREFIX}, fieldType.getId().getBytes()));
        String formatted = keyFormatter.formatKeyValue(kv);
        Assert.assertEquals(kv, keyFormatter.unformatKeyValue(formatted));
    }
}
