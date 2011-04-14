package org.lilyproject.indexer.engine;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * This is an interface for SolrServer (which is an abstract class).
 */
public interface SolrClient {
    /**
     * Description of this SOLR server, usually its URL.
     */
    String getDescription();

    //
    //
    // The following method declarations are copied from SOLR's SolrServer class, but with
    // InterruptedException added to their throws clause. This is necessary for the RetryingSolrClient.
    //
    //

    UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException, InterruptedException;

    UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException,
            InterruptedException;

    UpdateResponse deleteById(String id) throws SolrServerException, IOException, InterruptedException;

    UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException, InterruptedException;

    UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException, InterruptedException;

    UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException,
            InterruptedException;

    UpdateResponse commit() throws SolrServerException, IOException, InterruptedException;

    QueryResponse query(SolrParams params) throws SolrServerException, InterruptedException;
}
