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
package org.lilyproject.indexer.engine;

import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.lilyproject.indexer.model.indexerconf.Follow;
import org.lilyproject.indexer.model.indexerconf.IndexUpdateBuilder;
import org.lilyproject.indexer.model.indexerconf.RecordContext;
import org.lilyproject.indexer.model.indexerconf.Value;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

public class SolrDocumentBuilder implements IndexUpdateBuilder {

    private final Repository repository;
    private final TypeManager typeManager;
    private final ValueEvaluator valueEvaluator;

    private final SolrInputDocument solrDoc = new SolrInputDocument();
    private boolean emptyDocument = true;

    private RecordContext recordContext;

    private RecordId recordId;
    private String key;
    private SchemaId vtag;
    private long version;

    public SolrDocumentBuilder(Repository repository, ValueEvaluator valueEvaluator, IdRecord record, String key, SchemaId vtag, long version) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.valueEvaluator = valueEvaluator;
        this.recordId = record.getId();
        this.key = key;
        this.vtag = vtag;
        this.version = version;

        recordContext = new RecordContext(record);
    }

    public boolean isEmptyDocument() {
        return emptyDocument;
    }

    public SolrInputDocument build() throws InterruptedException, RepositoryException {
        solrDoc.setField("lily.id", recordId.toString());
        solrDoc.setField("lily.key", key);
        solrDoc.setField("lily.vtagId", vtag.toString());
        solrDoc.setField("lily.vtag", typeManager.getFieldTypeById(vtag).getName().getName());
        solrDoc.setField("lily.version", version);
        return solrDoc;
    }

    @Override
    public Repository getRepository() {
        return repository;
    }

    @Override
    public List<String> eval(Value value) throws RepositoryException, InterruptedException {
        return valueEvaluator.eval(value, recordContext, repository, vtag);
    }

    @Override
    public List evalFollow(Follow follow) throws RepositoryException, InterruptedException {
        return valueEvaluator.evalFollow(follow, recordContext.newFollow(), repository, vtag);
    }

    @Override
    public void addField(String fieldName, List<String> values) throws InterruptedException, RepositoryException {
        if (values != null) {
            for (String value: values) {
                solrDoc.addField(fieldName, value);
                emptyDocument = false;
            }
        }
    }

    @Override
    public RecordContext getRecordContext() {
        return recordContext;
    }

}
