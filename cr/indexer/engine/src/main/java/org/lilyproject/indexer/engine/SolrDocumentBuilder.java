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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;
import org.apache.solr.common.SolrInputDocument;
import org.lilyproject.indexer.engine.DerefMap.DependencyEntry;
import org.lilyproject.indexer.model.indexerconf.Dep;
import org.lilyproject.indexer.model.indexerconf.FieldTemplatePart;
import org.lilyproject.indexer.model.indexerconf.IndexUpdateBuilder;
import org.lilyproject.indexer.model.indexerconf.LiteralTemplatePart;
import org.lilyproject.indexer.model.indexerconf.NameTemplate;
import org.lilyproject.indexer.model.indexerconf.NameTemplateEvaluationException;
import org.lilyproject.indexer.model.indexerconf.NameTemplateResolver;
import org.lilyproject.indexer.model.indexerconf.RecordContext;
import org.lilyproject.indexer.model.indexerconf.TemplatePart;
import org.lilyproject.indexer.model.indexerconf.Value;
import org.lilyproject.indexer.model.indexerconf.VariantPropertyTemplatePart;
import org.lilyproject.repository.api.IdRecord;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

public class SolrDocumentBuilder implements IndexUpdateBuilder {

    private final Repository repository;
    private final TypeManager typeManager;
    private final ValueEvaluator valueEvaluator;
    private final NameTemplateResolver nameTemplateResolver;

    private final SolrInputDocument solrDoc = new SolrInputDocument();
    private boolean emptyDocument = true;

    private Stack<RecordContext> contexts;
    private LoadingCache<DependencyEntry, Set<SchemaId>> dependencies;

    private RecordId recordId;
    private String key;
    private SchemaId vtag;
    private long version;

    public SolrDocumentBuilder(Repository repository, ValueEvaluator valueEvaluator, IdRecord record, String key,
                               SchemaId vtag, long version) {
        this.repository = repository;
        this.typeManager = repository.getTypeManager();
        this.valueEvaluator = valueEvaluator;
        this.recordId = record.getId();
        this.key = key;
        this.vtag = vtag;
        this.version = version;

        this.nameTemplateResolver = new FieldNameTemplateResolver();

        this.contexts = new Stack<RecordContext>();
        this.push(record, new Dep(this.recordId, Collections.<String>emptySet()));

        this.dependencies = CacheBuilder.newBuilder().build(new CacheLoader<DependencyEntry, Set<SchemaId>>() {

            @Override
            public Set<SchemaId> load(DependencyEntry arg0) throws Exception {
                return Sets.newHashSet();
            }

        });
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
        return valueEvaluator.eval(value, this);
    }

    @Override
    public void addField(String fieldName, List<String> values) throws InterruptedException, RepositoryException {
        if (values != null) {
            for (String value : values) {
                solrDoc.addField(fieldName, value);
                emptyDocument = false;
            }
        }
    }

    @Override
    public RecordContext getRecordContext() {
        return contexts.peek();
    }

    @Override
    public NameTemplateResolver getFieldNameResolver() {
        return nameTemplateResolver;
    }

    private class FieldNameTemplateResolver implements NameTemplateResolver {

        @Override
        public Object resolve(TemplatePart part) {
            RecordContext ctx = contexts.peek();
            //TODO: add dependencies caused by resolving name template variables
            if (part instanceof FieldTemplatePart) {
                QName fieldName = ((FieldTemplatePart) part).getFieldType().getName();
                if (ctx.record.hasField(fieldName)) {
                    return ctx.record.getField(fieldName);
                } else {
                    throw new NameTemplateEvaluationException(
                            "Error evaluating name template: Record does not have field " + fieldName);
                }
            } else if (part instanceof VariantPropertyTemplatePart) {
                VariantPropertyTemplatePart vpPart = (VariantPropertyTemplatePart) part;
                return contexts.peek().contextRecord.getId().getVariantProperties().get(vpPart.getName());
            } else if (part instanceof LiteralTemplatePart) {
                return ((LiteralTemplatePart) part).getString();
            } else {
                throw new NameTemplateEvaluationException("Unsupported TemplatePart type " + part.getClass().getName());
            }
        }

    }

    @Override
    public void addDependency(SchemaId field) {
        RecordContext ctx = contexts.peek();
        try {
            if (!ctx.dep.vprops.isEmpty() || !ctx.dep.id.equals(recordId)) { // avoid adding unnecesary self-references
                dependencies.get(DerefMapUtil.newEntry(ctx.dep.id, ctx.dep.vprops)).add(field);
            }
        } catch (ExecutionException ee) {
            throw new RuntimeException("Failed to update dependencies");
        }
    }

    public Map<DependencyEntry, Set<SchemaId>> getDependencies() {
        return dependencies.asMap();
    }

    @Override
    public void push(Record record, Dep dep) {
        this.contexts.push(new RecordContext(record, dep));
    }

    @Override
    public void push(Record record, Record contextRecord, Dep dep) {
        this.contexts.push(new RecordContext(record, contextRecord, dep));
    }

    @Override
    public RecordContext pop() {
        return this.contexts.pop();
    }

    @Override
    public SchemaId getVTag() {
        return vtag;
    }

    @Override
    public String evalIndexFieldName(NameTemplate nameTemplate) {
        if (getRecordContext().record != null) {
            try {
                return nameTemplate.format(getFieldNameResolver());
            } catch (NameTemplateEvaluationException ntve) {
                return null;
            }
        } else {
            // collect dependencies introducted by any 'FieldTemplateParts'
            for (TemplatePart part : nameTemplate.getParts()) {
                if (part instanceof FieldTemplatePart) {
                    addDependency(((FieldTemplatePart) part).getFieldType().getId());
                }
            }

            return null;
        }
    }

}
