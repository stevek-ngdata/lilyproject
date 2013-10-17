package org.lilyproject.indexer.hbase.mapper;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.hbaseindex.IndexNotFoundException;
import org.lilyproject.indexer.derefmap.DependencyEntry;
import org.lilyproject.indexer.derefmap.DerefMap;
import org.lilyproject.indexer.derefmap.DerefMapHbaseImpl;
import org.lilyproject.indexer.engine.SolrDocumentBuilder;
import org.lilyproject.indexer.engine.ValueEvaluator;
import org.lilyproject.indexer.model.indexerconf.DynamicFieldNameTemplateResolver;
import org.lilyproject.indexer.model.indexerconf.DynamicIndexField;
import org.lilyproject.indexer.model.indexerconf.IndexCase;
import org.lilyproject.indexer.model.indexerconf.IndexField;
import org.lilyproject.indexer.model.indexerconf.IndexerConf;
import org.lilyproject.indexer.model.indexerconf.IndexerConfBuilder;
import org.lilyproject.indexer.model.indexerconf.IndexerConfException;
import org.lilyproject.indexer.model.indexerconf.MappingNode;
import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.FieldTypeNotFoundException;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.impl.RecordDecoder;
import org.lilyproject.repository.impl.id.AbsoluteRecordIdImpl;
import org.lilyproject.repository.impl.id.SchemaIdImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.hbase.RepoAndTableUtil;
import org.lilyproject.util.repo.VTaggedRecord;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.lilyproject.util.zookeeper.ZooKeeperImpl;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LilyResultToSolrMapper implements ResultToSolrMapper,Configurable {
    private final Log log = LogFactory.getLog(getClass());

    private String repositoryName;
    private String tableName;
    private String indexerConfString;
    private String indexName;

    private ZooKeeperItf zooKeeperItf;
    private RecordDecoder recordDecoder;
    private RepositoryManager repositoryManager;
    private LRepository repository;
    private LTable table;
    private IndexerConf indexerConf;
    private ValueEvaluator valueEvaluator;
    private DerefMap derefMap;

    @Override
    public void configure(Map<String, String> params) {
        try {
            zooKeeperItf = new ZooKeeperImpl(params.get("zookeeper"), 40000);
            setIndexName(params.get("name"));
            setRepositoryName(params.containsKey("repository") ? params.get("repository") : null);
            setTableName(params.containsKey("table") ? params.get("table") : null);
            setIndexerConfString(params.get("indexerConf"));

            setRepositoryManager(new LilyClient(zooKeeperItf));
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
        } catch (IndexerConfException e) {
            log.error(e);
        } catch (IndexNotFoundException e) {
            log.error(e);
        }
    }

    protected void init () throws RepositoryException, InterruptedException, IndexerConfException,
            IndexNotFoundException, UnsupportedEncodingException, IOException {
        repository = repositoryManager.getRepository(repositoryName != null ? repositoryName : RepoAndTableUtil.DEFAULT_REPOSITORY);
        table = repository.getTable(tableName != null ? tableName : LilyHBaseSchema.Table.RECORD.name);

        ByteArrayInputStream is = new ByteArrayInputStream(indexerConfString.getBytes("UTF-8"));
        indexerConf = IndexerConfBuilder.build(is, repository);

        valueEvaluator = new ValueEvaluator(indexerConf);
        recordDecoder = new RecordDecoder(repository.getTypeManager(), repository.getIdGenerator(), repository.getRecordFactory());
        derefMap = indexerConf.containsDerefExpressions() ?
                DerefMapHbaseImpl.create(repository.getRepositoryName(), indexName,
                        LilyClient.getHBaseConfiguration(zooKeeperItf), null, repository.getIdGenerator()) : null;
    }

    @Override
    public boolean containsRequiredData(Result result) {
        return false;
    }

    @Override
    public boolean isRelevantKV(final KeyValue kv) {
        return true;
        /*
        if (!Arrays.equals(LilyHBaseSchema.RecordCf.DATA.bytes,kv.getFamily())) {
            return false;
        }
        final byte[] qualifier = kv.getQualifier();
        if (qualifier[0] == LilyHBaseSchema.RecordColumn.DATA_PREFIX) {
            SchemaId fieldId = new SchemaIdImpl(Bytes.tail(qualifier, qualifier.length - 1));
            FieldType fieldType  = null;
            try {
                fieldType = repository.getTypeManager().getFieldTypeById(fieldId);
            } catch (IllegalArgumentException e) {
                // no qualifier
                return false;
            } catch (FieldTypeNotFoundException e) {
                // field doesn't exist
                return false;
            } catch (RepositoryException e) {
                log.warn(e);
            } catch (InterruptedException e) {
                log.warn(e);
            }

            if (fieldType == null) {
                return false;
            }

            StaticFieldTypeFinder finder = new StaticFieldTypeFinder(fieldType);
            indexerConf.getIndexFields().visitAll(finder);
            if (finder.foundRelevant) {
                return true;
            }

            for (DynamicIndexField indexField : indexerConf.getDynamicFields()) {
                if (indexField.matches(fieldType).match) {
                    return true;
                }
            }
        } else if (qualifier[0] == LilyHBaseSchema.RecordColumn.SYSTEM_PREFIX) {            
            List<LilyHBaseSchema.RecordColumn> columns = Arrays.asList(LilyHBaseSchema.RecordColumn.values());
            Collection<LilyHBaseSchema.RecordColumn> filtered = Collections2.filter(columns,
                    new Predicate<LilyHBaseSchema.RecordColumn>() {
                @Override
                public boolean apply(LilyHBaseSchema.RecordColumn input) {
                    return Arrays.equals(input.bytes, qualifier);
                }
            });
            return !filtered.isEmpty();
        } else {
            // is this a lily key value?
        }


        return false;
        */
    }

    @Override
    public Get getGet(byte[] row) {
        return new Get(row);
    }

    @Override
    public SolrInputDocument map(Result result) {
        SolrInputDocument solrInputDocument = null;
        try {
            Record record = recordDecoder.decodeRecord(result);

            IndexCase indexCase = indexerConf.getIndexCase(table.getTableName(), record);
            if (indexCase == null) {
                return solrInputDocument;
            }

            VTaggedRecord vtRecord = new VTaggedRecord(record.getId(), table, repository);
            IdRecord idRecord = vtRecord.getRecord();

            Set<SchemaId> vtags = Sets.intersection(indexCase.getVersionTags(), vtRecord.getVTags().keySet());

            for (SchemaId vtag : vtags) {
                SolrDocumentBuilder solrDocumentBuilder = new SolrDocumentBuilder(repository, indexerConf.getRecordFilter(),
                        indexerConf.getSystemFields(), valueEvaluator, table.getTableName(), idRecord,
                        getIndexId(table.getTableName(), record.getId(), vtag), vtag, vtRecord.getVTags().get(vtag));

                indexerConf.getIndexFields().collectIndexUpdate(solrDocumentBuilder);

                if (!indexerConf.getDynamicFields().isEmpty()) {
                    for (Map.Entry<SchemaId, Object> field : idRecord.getFieldsById().entrySet()) {
                        FieldType fieldType = repository.getTypeManager().getFieldTypeById(field.getKey());
                        for (DynamicIndexField dynField : indexerConf.getDynamicFields()) {
                            DynamicIndexField.DynamicIndexFieldMatch match = dynField.matches(fieldType);
                            if (match.match) {
                                String fieldName = evalName(dynField, match, fieldType);

                                List<String> values = valueEvaluator.format(table.getTableName(), record, fieldType, dynField.extractContext(),
                                        dynField.getFormatter(), repository);

                                solrDocumentBuilder.addField(fieldName, values);

                                if (!dynField.getContinue()) {
                                    // stop on first match, unless continue attribute is true
                                    break;
                                }
                            }
                        }
                    }
                }

                processDependencies(table.getTableName(), idRecord, vtag, solrDocumentBuilder);
                if (!solrDocumentBuilder.isEmptyDocument()) {
                    // TODO support multi doc outputting
                    solrInputDocument = solrDocumentBuilder.build();
                }
            }
        } catch (InterruptedException e) {
            log.warn(e);
        } catch (RepositoryException e) {
            log.warn(e);
        } catch (IOException e) {
            log.warn(e);
        }
        solrInputDocument.removeField("lily.key");
        return solrInputDocument;
    }

    protected static String getIndexId(String table, RecordId recordId, SchemaId vtag) {
        return table + "-" + recordId + "-" + vtag.toString();
    }

    private String evalName(DynamicIndexField dynField, DynamicIndexField.DynamicIndexFieldMatch match, FieldType fieldType) {
        // Calculate the name, then add the value
        Map<String, Object> nameContext = new HashMap<String, Object>();
        nameContext.put("namespace", fieldType.getName().getNamespace());
        nameContext.put("name", fieldType.getName().getName());

        ValueType valueType = fieldType.getValueType();
        nameContext.put("type", formatValueTypeName(valueType));
        nameContext.put("baseType", valueType.getBaseName().toLowerCase());

        // If there's no nested value type, revert to the current value type. This is practical for dynamic
        // fields that match on types like "*,LIST<+>".
        ValueType nestedValueType = valueType.getNestedValueType() != null ? valueType.getNestedValueType() : valueType;
        nameContext.put("nestedType", formatValueTypeName(nestedValueType));
        nameContext.put("nestedBaseType", nestedValueType.getBaseName().toLowerCase());

        nameContext.put("deepestNestedBaseType", valueType.getDeepestValueType().getBaseName().toLowerCase());

        boolean isList = valueType.getBaseName().equals("LIST");
        nameContext.put("multiValue", isList);
        nameContext.put("list", isList);

        if (match.nameMatch != null) {
            nameContext.put("nameMatch", match.nameMatch);
        }
        if (match.namespaceMatch != null) {
            nameContext.put("namespaceMatch", match.namespaceMatch);
        }
        return dynField.getNameTemplate().format(new DynamicFieldNameTemplateResolver(nameContext));
    }

    private String formatValueTypeName(ValueType valueType) {
        StringBuilder builder = new StringBuilder();

        while (valueType != null) {
            if (builder.length() > 0) {
                builder.append("_");
            }
            builder.append(valueType.getBaseName().toLowerCase());
            valueType = valueType.getNestedValueType();
        }

        return builder.toString();
    }

    private void processDependencies(String table, IdRecord record, SchemaId vtag, SolrDocumentBuilder solrDocumentBuilder)
            throws IOException, RepositoryException, InterruptedException {
        if (log.isDebugEnabled()) {
            log.debug("Constructed Solr doc: " + solrDocumentBuilder.build());
            log.debug("Updating dependencies for " + record.getId());
            logDependencies(record.getId(), solrDocumentBuilder.getDependencies());
        }

        if (derefMap != null) {
            derefMap.updateDependants(new AbsoluteRecordIdImpl(table, record.getId()),
                    vtag, solrDocumentBuilder.getDependencies());
        }
    }

    private void logDependencies(RecordId recordId, Map<DependencyEntry, Set<SchemaId>> dependencies) {
        for (DependencyEntry entry : dependencies.keySet()) {
            StringBuilder line = new StringBuilder();
            line.append(recordId).append(" | ")
                    .append(entry.getDependency())
                    .append("|").append(entry.getMoreDimensionedVariants())
                    .append("|").append(dependencies.get(entry));
            log.debug(line.toString());
        }
    }

    private static class StaticFieldTypeFinder implements Predicate<MappingNode> {
        boolean foundRelevant = false;
        final FieldType fieldType;
        private StaticFieldTypeFinder(FieldType fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public boolean apply(MappingNode mappingNode) {
            if (mappingNode instanceof IndexField) {
                IndexField indexField = (IndexField)mappingNode;
                if (fieldType.equals(indexField.getValue().getTargetFieldType())) {
                    foundRelevant = true;
                }
            }
            return !foundRelevant;
        }
    }

    protected void setIndexerConfString(String indexerConfString) {
        this.indexerConfString = indexerConfString;
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

    protected void setIndexName(String indexName) {
        this.indexName = indexName;
    }
}