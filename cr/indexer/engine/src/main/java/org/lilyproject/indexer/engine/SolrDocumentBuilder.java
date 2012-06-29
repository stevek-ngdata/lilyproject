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
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.TypeManager;

public class SolrDocumentBuilder {

    private final TypeManager typeManager;

    private final SolrInputDocument solrDoc = new SolrInputDocument();
    private boolean emptyDocument = true;

    private RecordId recordId;
    private String key;
    private SchemaId vtag;
    private long version;

    public SolrDocumentBuilder(TypeManager typeManager, RecordId recordId, String key, SchemaId vtag, long version) {
        this.typeManager = typeManager;
        this.recordId = recordId;
        this.key = key;
        this.vtag = vtag;
        this.version = version;
    }

    public SolrDocumentBuilder fields(String fieldName, List<String> values) {
        if (values != null) {
            for (String value: values) {
                solrDoc.addField(fieldName, value);
                emptyDocument = false;
            }
        }

        return this;
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

}
