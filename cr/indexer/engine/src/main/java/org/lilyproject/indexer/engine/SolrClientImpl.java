package org.lilyproject.indexer.engine;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SolrClientImpl implements SolrClient {

    private SolrServer solrServer;

    private String description;

    public SolrClientImpl(SolrServer solrServer, String description) {
        this.solrServer = solrServer;
        this.description = description;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
        return solrServer.add(doc);
    }

    @Override
    public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
        return solrServer.add(docs);
    }

    @Override
    public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
        return solrServer.deleteById(ids);
    }

    @Override
    public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
        return solrServer.deleteById(id);
    }

    @Override
    public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
        return solrServer.deleteByQuery(query);
    }

    @Override
    public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
        return solrServer.commit(waitFlush, waitSearcher);
    }

    @Override
    public UpdateResponse commit() throws SolrServerException, IOException {
        return solrServer.commit();
    }

    @Override
    public QueryResponse query(SolrParams params) throws SolrServerException {
        return solrServer.query(params);
    }
}
