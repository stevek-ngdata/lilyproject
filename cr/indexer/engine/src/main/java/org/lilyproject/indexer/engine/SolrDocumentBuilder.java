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
    private SchemaId vtag;
    private long version;

    public SolrDocumentBuilder(TypeManager typeManager) {
        this.typeManager = typeManager;
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

    public SolrDocumentBuilder recordId(RecordId recordId) {
        this.recordId = recordId;
        return this;
    }

    public SolrDocumentBuilder vtag(SchemaId vtag) {
        this.vtag = vtag;
        return this;
    }

    public SolrDocumentBuilder version(long version) {
        this.version = version;
        return this;
    }

    public SolrInputDocument build() throws InterruptedException, RepositoryException {
        solrDoc.setField("lily.id", recordId.toString());
        solrDoc.setField("lily.key", getIndexId(recordId, vtag));
        solrDoc.setField("lily.vtagId", vtag.toString());
        solrDoc.setField("lily.vtag", typeManager.getFieldTypeById(vtag).getName().getName());
        solrDoc.setField("lily.version", version);
        return solrDoc;
    }

    // Keep in sync with Indexer.java
    // FIXME: remove duplication
    protected String getIndexId(RecordId recordId, SchemaId vtag) {
        return recordId + "-" + vtag.toString();
    }

}
