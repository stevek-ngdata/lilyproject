package org.lilyproject.indexer.hbase.mapper;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.repo.VersionTag;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZooKeeperImpl;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class LilyUniqueKeyFormatter implements UniqueKeyFormatter, Configurable{
    private final Log log = LogFactory.getLog(getClass());

    private IdGenerator idGenerator;
    private static final String SEPARATOR = "|";

    private RepositoryManager repositoryManager;
    private String repositoryName;
    private String tableName;
    private LRepository repository;
    private TypeManager typeManager;
    private SchemaId vtag;

    @Override
    public void configure(Map<String, String> params) {
        try {
            ZooKeeperItf zk = new ZooKeeperImpl(params.get("zookeeper"), 40000);
            repositoryManager = new LilyClient(zk);
            repositoryName = params.containsKey("repository") ? params.get("repository") : null;
            tableName = params.containsKey("table") ? params.get("table") : LilyHBaseSchema.Table.RECORD.name;

            init();
        } catch (IOException e) {
            log.error(e);
        } catch (InterruptedException e) {
            log.error(e);
        } catch (KeeperException e) {
            log.error(e);
        } catch (ZkConnectException e) {
            log.error(e);
        } catch (RepositoryException e) {
            log.error(e);
        }
    }

    protected void init () throws RepositoryException, InterruptedException{
        repository = repositoryManager.getRepository(repositoryName != null ?
                repositoryName : RepoAndTableUtil.DEFAULT_REPOSITORY);
        typeManager = repository.getTypeManager();
        idGenerator = repository.getIdGenerator();
        tableName = tableName != null ? tableName : LilyHBaseSchema.Table.RECORD.name;
        vtag = typeManager.getFieldTypeByName(VersionTag.LAST).getId();
    }

    @Override
    public String formatFamily(byte[] family) {
        return Bytes.toString(family);
    }

    @Override
    public String formatRow(byte[] row) {
        RecordId recordId = idGenerator.fromBytes(row);
        return LilyResultToSolrMapper.getIndexId(tableName, recordId, vtag);
    }

    @Override
    public String formatKeyValue(KeyValue keyValue) {
        //TODO what about versions?
        String row = formatRow(keyValue.getRow());
        String family = formatFamily(keyValue.getFamily());
        byte[] fieldKey = keyValue.getQualifier();
        String field = null;
        try {
            if (fieldKey[0] == LilyHBaseSchema.RecordColumn.DATA_PREFIX) {
                FieldType fieldType = typeManager.getFieldTypeById(new SchemaIdImpl(Bytes.tail(fieldKey, fieldKey.length - 1)));
                field = Bytes.toString(Bytes.add(new byte[] {LilyHBaseSchema.RecordColumn.DATA_PREFIX}
                        , Bytes.toBytes(fieldType.getName().toString())));
            } else if (fieldKey[0] == LilyHBaseSchema.RecordColumn.SYSTEM_PREFIX) {
                field = Bytes.toString(fieldKey);
            } else {
                log.warn("Unknown field type " + fieldKey + " (" + Bytes.toString(fieldKey) +")");
            }
        } catch (RepositoryException e) {
            log.warn(e);
        } catch (InterruptedException e) {
            log.warn(e);
        }

        if (row == null || family == null || field == null) {
            return null;
        }

        // TODO escaping!
        return Joiner.on(SEPARATOR).join(row,family,field);
    }

    @Override
    public byte[] unformatRow(String keyString) {
        String recordIdString = keyString.substring(tableName.length() + 1,
        keyString.length() - vtag.toString().length() - 1);
        return idGenerator.fromString(recordIdString).toBytes();
    }

    @Override
    public byte[] unformatFamily(String familyString) {
        return Bytes.toBytes(familyString);
    }

    @Override
    public KeyValue unformatKeyValue(String keyValueString) {
        //TODO unescaping
        List<String> parts = Lists.newArrayList(Splitter.on(SEPARATOR).split(keyValueString));

        // TODO checks on parts
        byte[] qualifier = null;


        byte[] fieldKey = Bytes.toBytes(parts.get(2));
        if (fieldKey[0] == LilyHBaseSchema.RecordColumn.DATA_PREFIX) {
            FieldType fieldType = null;
            try {
                String fieldName = Bytes.toString(fieldKey,1, fieldKey.length - 1);
                fieldType = typeManager.getFieldTypeByName(QName.fromString(fieldName));
                qualifier = Bytes.add(new byte[]{LilyHBaseSchema.RecordColumn.DATA_PREFIX}, fieldType.getId().getBytes());
            } catch (RepositoryException e) {
                log.warn(e);
            } catch (InterruptedException e) {
                log.warn(e);
            }
        } else {
            qualifier = fieldKey;
        }




        return new KeyValue(
                unformatRow(parts.get(0)),
                unformatFamily(parts.get(1)),
                qualifier
                );
    }

    protected void setRepositoryManager(RepositoryManager repositoryManager) {
        this.repositoryManager = repositoryManager;
    }

    protected void setRepositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    protected void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
